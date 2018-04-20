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
data <- read.table(header=T, file="runtime.dat")
datac <- summarySE(data, measurevar="runtime", groupvars=c("sys", "steps", "status"))

require(ggplot2)
require(scales)

lgLabels <- c("arabesque", "fractal")
cbPalette <- c("#999999", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")
lgBreaks <- c("arabesque", "fractal")

ggplot(datac, aes(x=steps, y=runtime, fill=factor(sys))) + 
    geom_bar(position=position_dodge(), size=10, stat="identity") +
    geom_text(aes(x = steps, y=max(runtime+100000), label = status, group = factor(sys)), position = position_dodge(width = 1)) +
    geom_errorbar(aes(ymin=(runtime-se), ymax=(runtime+se)),
                  colour="black", width=.005) +
    labs(title="Motifs-Youtube-Multi-Label", x="Step", y="Runtime (ms)", fill="") +
    scale_colour_manual(values=cbPalette, labels=lgLabels, breaks=lgBreaks) +
    theme_minimal(base_size = 20) +
    theme(legend.title=element_blank())

ggsave(file="motifs_runtime.pdf", family="serif", heigh=4, width=7)
ggsave(file="motifs_runtime.png", family="serif", heigh=4, width=7)

ggplot(datac, aes(x=steps, y=ifelse(runtime>0,runtime,1), fill=factor(sys))) + 
    geom_bar(position=position_dodge(), size=10, stat="identity") +
    geom_text(aes(x = steps, y=max(runtime+10000000), label = status, group = factor(sys)), position = position_dodge(width = 1)) +
    geom_errorbar(aes(ymin=(runtime-se), ymax=(runtime+se)),
                  colour="black", width=.005) +
    labs(title="Motifs-Youtube-Multi-Label (log)", x="Step", y="Runtime (ms)", fill="") +
    scale_colour_manual(values=cbPalette, labels=lgLabels, breaks=lgBreaks) +
    scale_y_log10() +
    theme_minimal(base_size = 20) +
    theme(legend.title=element_blank())


ggsave(file="motifs_runtime_log.pdf", family="serif", heigh=4, width=7)
ggsave(file="motifs_runtime_log.png", family="serif", heigh=4, width=7)
