require(ggplot2)
require(scales)
require(dplyr)
require(cowplot)

data <- read.table(header=T, file="breakdown.dat")
stepruntime <- data %>% group_by(mode, step, partitionid) %>%
   summarise(runtime=sum(runtime)) %>%
   group_by(mode, step) %>% summarise(runtimemax=max(runtime), runtimemin=min(runtime))
data <- merge(x = data, y = stepruntime, by = c("mode", "step"), all.x= TRUE, suffixes=c("",""))

scaleFUN <- function(x) sprintf("%8.1f", x)

## step 0
lgLabels <- c("Original work", "Work stealing")
lgValues <- c("#7570b3", "#1b9e77", "#d95f02")
lgBreaks <- c("InitialComputation", "WorkStealingComputation")

stepdata <- subset(data, step == 0 & mode == "1.Disabled")
stepdata$descmode <- paste(stepdata$mode, "\n", formatC(stepdata$runtimemax / 1000, digits=2, format="f"), "s", sep="")
disabled.0 <- ggplot(
      stepdata,
      aes(x=factor(partitionid), y=runtime/1000,
      fill=factor(breakdown, levels=c("WorkStealingComputation", "InitialComputation")))) + 
    geom_bar(stat="identity", position = "stack") +
    labs(x="Tasks", y="Runtime (seconds)") +
    scale_fill_brewer(palette="Paired", labels=lgLabels, breaks=lgBreaks) +
    scale_y_continuous(labels=scaleFUN) +
    facet_grid(.~descmode, scales="free") +
    theme_minimal(base_size = 10) +
    theme(legend.title=element_blank(),
          legend.position="none",
          axis.title.x=element_blank(),
          axis.text.x=element_blank(),
          axis.ticks.x=element_blank(),
          strip.text.y=element_text(angle=0)
          )

stepdata <- subset(data, step == 0 & mode != "1.Disabled")
stepdata$descmode <- paste(stepdata$mode, "\n", formatC(stepdata$runtimemax / 1000, digits=2, format="f"), "s", sep="")
print (head(stepdata))

plot.0 <- ggplot(
      stepdata,
      aes(x=factor(partitionid), y=runtime/1000,
      fill=factor(breakdown, levels=c("WorkStealingComputation", "InitialComputation")))) + 
    geom_bar(stat="identity", position = "stack") +
    labs(x="Tasks", y="Runtime (seconds)") +
    scale_fill_brewer(palette="Paired", labels=lgLabels, breaks=lgBreaks) +
    scale_y_continuous(labels=scaleFUN) +
    theme_minimal(base_size = 10) +
    facet_grid(.~descmode, scales="free") +
    theme(legend.title=element_blank(),
          legend.position="none",
          axis.title.x=element_blank(),
          axis.text.x=element_blank(),
          axis.ticks.x=element_blank(),
          axis.title.y=element_blank(),
          strip.text.y=element_text(angle=0)
          )

plot.legend <- get_legend(plot.0 + theme(legend.position="bottom"))
###

## step 1
lgLabels <- c("Original work", "Work Stealing")
lgValues <- c("#7570b3", "#1b9e77", "#d95f02")
lgBreaks <- c("InitialComputation", "WorkStealingComputation")

stepdata <- subset(data, step == 1 & mode == "1.Disabled")
stepdata$descmode <- paste(stepdata$mode, "\n", formatC(stepdata$runtimemax / 1000, digits=2, format="f"), "s", sep="")
disabled.1 <- ggplot(
      stepdata,
      aes(x=factor(partitionid), y=runtime/1000,
      fill=factor(breakdown, levels=c("WorkStealingComputation", "InitialComputation")))) + 
    geom_bar(stat="identity", position = "stack") +
    labs(x="Tasks", y="Runtime (seconds)") +
    scale_fill_brewer(palette="Paired", labels=lgLabels, breaks=lgBreaks) +
    scale_y_continuous(labels=scaleFUN) +
    facet_grid(.~descmode, scales="free") +
    theme_minimal(base_size = 10) +
    theme(legend.title=element_blank(),
          legend.position="none",
          axis.title.x=element_blank(),
          axis.text.x=element_blank(),
          axis.ticks.x=element_blank(),
          strip.text.y=element_text(angle=0)
          )

stepdata <- subset(data, step == 1 & mode != "1.Disabled")
stepdata$descmode <- paste(stepdata$mode, "\n", formatC(stepdata$runtimemax / 1000, digits=2, format="f"), "s", sep="")
print (head(stepdata))

plot.1 <- ggplot(
      stepdata,
      aes(x=factor(partitionid), y=runtime/1000,
      fill=factor(breakdown, levels=c("WorkStealingComputation", "InitialComputation")))) + 
    geom_bar(stat="identity", position = "stack") +
    labs(x="Tasks", y="Runtime (seconds)") +
    scale_fill_brewer(palette="Paired", labels=lgLabels, breaks=lgBreaks) +
    scale_y_continuous(labels=scaleFUN) +
    theme_minimal(base_size = 10) +
    facet_grid(.~descmode, scales="free") +
    theme(
          legend.position="none",
          axis.title.x=element_blank(),
          axis.text.x=element_blank(),
          axis.ticks.x=element_blank(),
          axis.title.y=element_blank(),
          strip.text.y=element_text(angle=0)
          )
###

## step 2
lgLabels <- c("Original work", "Work Stealing")
lgValues <- c("#7570b3", "#1b9e77", "#d95f02")
lgBreaks <- c("InitialComputation", "WorkStealingComputation")

stepdata <- subset(data, step == 2 & mode == "1.Disabled")
stepdata$descmode <- paste(stepdata$mode, "\n", formatC(stepdata$runtimemax / 1000, digits=2, format="f"), "s", sep="")
disabled.2 <- ggplot(
      stepdata,
      aes(x=factor(partitionid), y=runtime/1000,
      fill=factor(breakdown, levels=c("WorkStealingComputation", "InitialComputation")))) + 
    geom_bar(stat="identity", position = "stack") +
    labs(x="Tasks", y="Runtime (seconds)") +
    scale_fill_brewer(palette="Paired", labels=lgLabels, breaks=lgBreaks) +
    scale_y_continuous(labels=scaleFUN) +
    facet_grid(.~descmode, scales="free") +
    theme_minimal(base_size = 10) +
    theme(legend.title=element_blank(),
          legend.position="none",
          axis.title.x=element_blank(),
          axis.text.x=element_blank(),
          axis.ticks.x=element_blank(),
          strip.text.y=element_text(angle=0)
          )

stepdata <- subset(data, step == 2 & mode != "1.Disabled")
stepdata$descmode <- paste(stepdata$mode, "\n", formatC(stepdata$runtimemax / 1000, digits=2, format="f"), "s", sep="")
print (head(stepdata))

plot.2 <- ggplot(
      stepdata,
      aes(x=factor(partitionid), y=runtime/1000,
      fill=factor(breakdown, levels=c("WorkStealingComputation", "InitialComputation")))) + 
    geom_bar(stat="identity", position = "stack") +
    labs(x="Tasks", y="Runtime (seconds)") +
    scale_fill_brewer(palette="Paired", labels=lgLabels, breaks=lgBreaks) +
    scale_y_continuous(labels=scaleFUN) +
    theme_minimal(base_size = 10) +
    facet_grid(.~descmode, scales="free") +
    theme(
          legend.position="none",
          axis.title.x=element_blank(),
          axis.text.x=element_blank(),
          axis.ticks.x=element_blank(),
          axis.title.y=element_blank(),
          strip.text.y=element_text(angle=0)
          )
###

## step 3
lgLabels <- c("Original work", "Work Stealing")
lgValues <- c("#7570b3", "#1b9e77", "#d95f02")
lgBreaks <- c("InitialComputation", "WorkStealingComputation")

stepdata <- subset(data, step == 3 & mode == "1.Disabled")
stepdata$descmode <- paste(stepdata$mode, "\n", formatC(stepdata$runtimemax / 1000, digits=2, format="f"), "s", sep="")
disabled.3 <- ggplot(
      stepdata,
      aes(x=factor(partitionid), y=runtime/1000,
      fill=factor(breakdown, levels=c("WorkStealingComputation", "InitialComputation")))) + 
    geom_bar(stat="identity", position = "stack") +
    labs(x="Tasks", y="Runtime (seconds)") +
    scale_fill_brewer(palette="Paired", labels=lgLabels, breaks=lgBreaks) +
    scale_y_continuous(labels=scaleFUN) +
    facet_grid(.~descmode, scales="free_y") +
    theme_minimal(base_size = 10) +
    theme(legend.title=element_blank(),
          legend.position="none",
          axis.title.x=element_blank(),
          axis.text.x=element_blank(),
          axis.ticks.x=element_blank(),
          strip.text.y=element_text(angle=0)
          )

stepdata <- subset(data, step == 3 & mode != "1.Disabled")
stepdata$descmode <- paste(stepdata$mode, "\n", formatC(stepdata$runtimemax / 1000, digits=2, format="f"), "s", sep="")
print (head(stepdata))

plot.3 <- ggplot(
      stepdata,
      aes(x=factor(partitionid), y=runtime/1000,
      fill=factor(breakdown, levels=c("WorkStealingComputation", "InitialComputation")))) + 
    geom_bar(stat="identity", position = "stack") +
    labs(x="Tasks", y="Runtime (seconds)") +
    scale_fill_brewer(palette="Paired", labels=lgLabels, breaks=lgBreaks) +
    scale_y_continuous(labels=scaleFUN) +
    theme_minimal(base_size = 10) +
    facet_grid(.~descmode, scales="free") +
    theme(
          legend.position="none",
          axis.title.x=element_blank(),
          axis.text.x=element_blank(),
          axis.ticks.x=element_blank(),
          axis.title.y=element_blank(),
          strip.text.y=element_text(angle=0)
          )
###


## step 4
lgLabels <- c("Original work", "Work Stealing")
lgValues <- c("#7570b3", "#1b9e77", "#d95f02")
lgBreaks <- c("InitialComputation", "WorkStealingComputation")

stepdata <- subset(data, step == 4 & mode == "1.Disabled")
stepdata$descmode <- paste(stepdata$mode, "\n", formatC(stepdata$runtimemax / 1000, digits=2, format="f"), "s", sep="")
disabled.4 <- ggplot(
      stepdata,
      aes(x=factor(partitionid), y=runtime/1000,
      fill=factor(breakdown, levels=c("WorkStealingComputation", "InitialComputation")))) + 
    geom_bar(stat="identity", position = "stack") +
    labs(x="Tasks", y="Runtime (seconds)") +
    scale_fill_brewer(palette="Paired", labels=lgLabels, breaks=lgBreaks) +
    scale_y_continuous(labels=scaleFUN) +
    facet_grid(.~descmode, scales="free_y") +
    theme_minimal(base_size = 10) +
    theme(legend.title=element_blank(),
          legend.position="none",
          axis.title.x=element_blank(),
          axis.text.x=element_blank(),
          axis.ticks.x=element_blank(),
          strip.text.y=element_text(angle=0)
          )

stepdata <- subset(data, step == 4 & mode != "1.Disabled")
stepdata$descmode <- paste(stepdata$mode, "\n", formatC(stepdata$runtimemax / 1000, digits=2, format="f"), "s", sep="")
print (head(stepdata))

plot.4 <- ggplot(
      stepdata,
      aes(x=factor(partitionid), y=runtime/1000,
      fill=factor(breakdown, levels=c("WorkStealingComputation", "InitialComputation")))) + 
    geom_bar(stat="identity", position = "stack") +
    labs(x="Tasks", y="Runtime (seconds)") +
    scale_fill_brewer(palette="Paired", labels=lgLabels, breaks=lgBreaks) +
    scale_y_continuous(labels=scaleFUN) +
    theme_minimal(base_size = 10) +
    facet_grid(.~descmode, scales="free") +
    theme(
          legend.position="none",
          axis.title.x=element_blank(),
          axis.text.x=element_blank(),
          axis.ticks.x=element_blank(),
          axis.title.y=element_blank(),
          strip.text.y=element_text(angle=0)
          )
###

grid_plot <- plot_grid(
                       plot.legend,
                       ggplot(),
                       disabled.0,
                       plot.0,
                       disabled.1,
                       plot.1,
                       disabled.2,
                       plot.2,
                       disabled.3,
                       plot.3,
                       disabled.4,
                       plot.4,
                       nrow=6,
                       ncol=2,
                       label_size=10,
                       align="v",
                       axis="l",
                       rel_heights=c(.2, 1, 1, 1, 1, 1),
                       rel_widths=c(0.48, 1),
                       labels=c("","","Step 0", "","Step 1", "","Step 2", "","Step 3", "","Step 4"))

save_plot(file="breakdown-grid.pdf", grid_plot, family="serif", base_heigh=8, base_width=6)
save_plot(file="breakdown-grid.png", grid_plot, family="serif", base_heigh=8, base_width=6)
