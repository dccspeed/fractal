## Summarizes data.
## Gives count, mean, standard deviation, standard error of the mean, and
## confidence interval (default 95%).
##   data: a data frame.
##   measurevar: the name of a column that contains the variable to be
##   summariezed
##   groupvars: a vector containing names of columns that contain grouping
##   variables
##   na.rm: a boolean that indicates whether to ignore NA's
##   conf.interval: the percent range of the confidence interval (default is
##   95%)
summarySE <- function(data=NULL, measurevar, groupvars=NULL, na.rm=FALSE,
                      conf.interval=.95, .drop=TRUE) {
   require(plyr)

   # New version of length which can handle NA's: if na.rm==T, don't count them
   length2 <- function (x, na.rm=FALSE) {
      if (na.rm) sum(!is.na(x))
      else       length(x)
   }

   # This does the summary. For each group's data frame, return a vector
   # with
   # N, mean, and sd
   datac <- ddply(data, groupvars, .drop=.drop,
                  .fun = function(xx, col) {
                     c(N    = length2(xx[[col]], na.rm=na.rm),
                       mean = mean   (xx[[col]], na.rm=na.rm),
                       sd   = sd     (xx[[col]], na.rm=na.rm)
                       )
                  },
                  measurevar
                  )

   # Rename the "mean" column    
   datac <- rename(datac, c("mean" = measurevar))

   datac$se <- datac$sd / sqrt(datac$N)  # Calculate standard error of the mean

   # Confidence interval multiplier for standard error
   # Calculate t-statistic for confidence interval: 
   # e.g., if conf.interval is .95, use .975 (above/below), and use
   # df=N-1
   ciMult <- qt(conf.interval/2 + .5, datac$N-1)
   datac$ci <- datac$se * ciMult

   return(datac)
}

# original data
datac <- read.table(header=T, file="motifs-mico.dat")

print(datac)

require(ggplot2)
require(scales)

lgLabels <- c("Previous steps", "Current step")
lgValues <- c("#7570b3", "#1b9e77", "#d95f02")
lgBreaks <- c("prevsteps", "laststep")

ggplot(datac, aes(x=nvertices, y=nembeddings, fill=factor(group, levels=c("prevsteps", "laststep")), label=sprintf("%.1f %%", relcost*100))) + 
    geom_bar(position=position_stack(), size=10, stat="identity") +
    geom_text(size = 5.5, position = position_stack(vjust = 0.5), color="white") + 
    labs(x="# Vertices", y="# Subgraphs -- log-scale") +
    scale_fill_manual(values=lgValues, labels=lgLabels, breaks=lgBreaks) +
    #theme_minimal() +
    scale_y_log10() +
    theme_classic(base_size = 22) +
    theme(legend.title=element_blank(), legend.position="top")

ggsave(file="cost_motifs_mico_runtime.pdf", family="serif", heigh=4, width=6)
ggsave(file="cost_motifs_mico_runtime.png", family="serif", heigh=4, width=6)
