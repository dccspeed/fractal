library(ggplot2)

data <- read.table('word-distributions.dat', header=T)
cbPalette <- c("#999999", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")

nsteps <- length(unique(data$step))

for (curr_step in 0:(nsteps-1)) {
   for (curr_depth in 0:curr_step) {
      ggplot() +
         geom_line(data=subset(data, step==curr_step & depth==curr_depth), aes(x=word, y=rfreq, colour=factor(depth))) +
         labs(x='Word ID', y='Relative Frequency') +
         scale_colour_manual(values=cbPalette, name="Depth") +
         theme_minimal()

      ggsave(file=paste("word-distributions-", curr_step, "-", curr_depth, ".pdf", sep=""), family="serif", heigh=4, width=6)
      ggsave(file=paste("word-distributions-", curr_step, "-", curr_depth, ".png", sep=""), family="serif", heigh=4, width=6)
   }

   ggplot() +
      geom_line(data=subset(data, step==curr_step), aes(x=word, y=rfreq, colour=factor(depth))) +
      labs(x='Word ID', y='Relative Frequency') +
      scale_colour_manual(values=cbPalette, name="Depth") +
      theme_minimal()

   ggsave(file=paste("word-distributions-", curr_step, ".pdf", sep=""), family="serif", heigh=4, width=6)
   ggsave(file=paste("word-distributions-", curr_step, ".png", sep=""), family="serif", heigh=4, width=6)

   ggplot() +
      stat_ecdf(data=subset(data, step==curr_step, depth=curr_step), aes(x=freq), geom="step") +
      labs(x='Frequency', y='% Active Words') +
      scale_colour_manual(values=cbPalette, name="Depth") +
      theme_minimal()

   ggsave(file=paste("word-distributions-cdf-", curr_step, ".pdf", sep=""), family="serif", heigh=4, width=6)
   ggsave(file=paste("word-distributions-cdf-", curr_step, ".png", sep=""), family="serif", heigh=4, width=6)
}

