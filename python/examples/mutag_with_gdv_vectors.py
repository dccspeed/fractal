import os.path as osp
import time

import torch
import torch.nn.functional as F
from torch.nn import Linear
from torch_geometric.datasets import TUDataset
from torch_geometric.loader import DataLoader
from torch_geometric.nn import GCNConv, GraphMultisetTransformer

import pyfractal as pf
from pyfractal.model import FractalContext

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

class Net(torch.nn.Module):
    def __init__(self, num_features, num_classes):
        super().__init__()

        self.conv1 = GCNConv(num_features, 32)
        self.conv2 = GCNConv(32, 32)
        self.conv3 = GCNConv(32, 32)

        self.pool = GraphMultisetTransformer(96, k=10, heads=4)

        self.lin1 = Linear(96, 16)
        self.lin2 = Linear(16, num_classes)

    def forward(self, x0, edge_index, batch):
        x1 = self.conv1(x0, edge_index).relu()
        x2 = self.conv2(x1, edge_index).relu()
        x3 = self.conv3(x2, edge_index).relu()
        x = torch.cat([x1, x2, x3], dim=-1)

        x = self.pool(x, batch)

        x = self.lin1(x).relu()
        x = F.dropout(x, p=0.5, training=self.training)
        x = self.lin2(x)

        return x


def train(model, optimizer, train_loader):
    model.train()

    total_loss = 0
    for data in train_loader:
        data = data.to(device)
        optimizer.zero_grad()
        out = model(data.x, data.edge_index, data.batch)
        loss = F.cross_entropy(out, data.y)
        loss.backward()
        total_loss += data.num_graphs * float(loss)
        optimizer.step()
    return total_loss / len(train_loader.dataset)


@torch.no_grad()
def test(model, loader):
    model.eval()

    total_correct = 0
    for data in loader:
        data = data.to(device)
        out = model(data.x, data.edge_index, data.batch)
        total_correct += int((out.argmax(dim=-1) == data.y).sum())
    return total_correct / len(loader.dataset)


spark = pf.DefaultSparkBuilder() \
    .master("local[8]") \
    .config("spark.driver.memory", "2g") \
    .appName("demo") \
    .getOrCreate()
fc = FractalContext(spark)


def build_graphlet_degree_vectors_with_fractal(data):
    fg = fc.unlabeled_graph_from_pyg_data(data)
    x = fg.graphlet_degree_vectors(5)
    return x


# get MUTAG dataset
path = osp.dirname(osp.realpath(__file__))
dataset = TUDataset(path, name="MUTAG").shuffle()
num_classes = dataset.num_classes

# compute graphlet degree vectors
loader = DataLoader(dataset, batch_size=len(dataset), shuffle=False)
full_batched_data = next(iter(loader))
start = time.time()
full_batched_data_x = build_graphlet_degree_vectors_with_fractal(full_batched_data)
elapsed = time.time() - start
print(f"Time to get GDV features using Fractal: {elapsed} seconds")

# apply GDV features to a copy of the dataset
dataset_with_gdv_features = []
for i in range(len(dataset)):
   data = dataset[i]
   from_idx = full_batched_data.ptr[i]
   to_idx = full_batched_data.ptr[i + 1]
   x = full_batched_data_x[from_idx:to_idx]
   data.x = x
   dataset_with_gdv_features.append(data)


def train_test_dataset(dataset):
    num_features = dataset[0].x.shape[1]

    n = (len(dataset) + 9) // 10
    train_dataset = dataset[2 * n:]
    val_dataset = dataset[n:2 * n]
    test_dataset = dataset[:n]

    print(f"train={len(dataset)} val={len(val_dataset)} test={len(test_dataset)}")

    nruns = 5
    for run in range(nruns):
        train_loader = DataLoader(train_dataset, batch_size=len(train_dataset), shuffle=True)
        val_loader = DataLoader(val_dataset, batch_size=len(val_dataset))
        test_loader = DataLoader(test_dataset, batch_size=len(test_dataset))
        model = Net(num_features, num_classes).to(device)
        optimizer = torch.optim.Adam(model.parameters(), lr=0.001, weight_decay=1e-4)
        accs = []
        for epoch in range(1, 201):
            train_loss = train(model, optimizer, train_loader)
            val_acc = test(model, val_loader)
            test_acc = test(model, test_loader)
        accs.append(test_acc)
        print(f'Train Loss: {train_loss:.2f} Val Acc: {val_acc:.2f} Test Acc: {test_acc:.2f}')
    print(f"Avg Test Acc: {torch.tensor(accs).mean():.2f}")


print("\n== Model with default features ==")
train_test_dataset(dataset)

print("\n== Model with Graphlet Degree Vector Features ==")
train_test_dataset(dataset_with_gdv_features)

fc.stop()
spark.stop()
