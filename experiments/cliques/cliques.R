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
data <- read.table(header=T, file="cliques.dat")
datac <- summarySE(data, measurevar="runtime", groupvars=c("comm", "step"))

require(ggplot2)
require(extrafont)
require(scales)
loadfonts()

lgLabels <- c("Scratch", "Btag")
cbPalette <- c("#999999", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")
lgBreaks <- c("scratch", "btag")

ggplot(datac, aes(x=step, y=runtime, fill=factor(comm))) + 
    geom_bar(position=position_dodge(), size=10, stat="identity") +
    geom_errorbar(aes(ymin=(runtime-se), ymax=(runtime+se)),
                  colour="black", width=.005) +
    labs(title="Cliques", x="Step", y="Runtime (ms)", fill="") +
    scale_colour_manual(values=cbPalette, labels=lgLabels, breaks=lgBreaks) +
    # scale_y_log10() +
    #scale_x_continuous(breaks=c(1,2,3,4)) +
    #scale_shape_manual (name="", values=c(22, 23)) +
    theme_minimal(base_size = 20) +
    theme(legend.title=element_blank())

ggsave(file="cliques_runtime.pdf", family="serif", heigh=4, width=6)
ggsave(file="cliques_runtime.png", family="serif", heigh=4, width=6)

ggplot(datac, aes(x=step, y=runtime, fill=factor(comm))) + 
    geom_bar(position=position_dodge(), size=10, stat="identity") +
    geom_errorbar(aes(ymin=(runtime-se), ymax=(runtime+se)),
                  colour="black", width=.005) +
    labs(title="Cliques (log)", x="Step", y="Runtime (ms)", fill="") +
    scale_colour_manual(values=cbPalette, labels=lgLabels, breaks=lgBreaks) +
    scale_y_log10() +
    #scale_x_continuous(breaks=c(1,2,3,4)) +
    #scale_shape_manual (name="", values=c(22, 23)) +
    theme_minimal(base_size = 20) +
    theme(legend.title=element_blank())


ggsave(file="cliques_runtime_log.pdf", family="serif", heigh=4, width=6)
ggsave(file="cliques_runtime_log.png", family="serif", heigh=4, width=6)
