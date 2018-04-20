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

require(ggplot2)
require(scales)
require(dplyr)

datac <- read.table(header=T, file="breakdown.dat")

stepruntime <- datac %>% group_by(mode, step, partitionid) %>%
   summarise(runtime=sum(runtime)) %>%
   group_by(mode, step) %>% summarise(runtimemax=max(runtime), runtimemin=min(runtime))

print (stepruntime)

datac <- merge(x = datac, y = stepruntime, by = c("mode", "step"), all.x= TRUE, suffixes=c("",""))
datac$descstep <- paste("(Step ", datac$step, ") (", formatC(datac$runtimemax / 1000, digits=2, format="f"), "s)", sep="")

print (head(datac))

lgLabels <- c("Original work", "Work stealing")
lgValues <- c("#7570b3", "#1b9e77", "#d95f02")
lgBreaks <- c("InitialComputation", "WorkStealingComputation")

ggplot(datac, aes(x=factor(partitionid), y=runtime/1000, fill=factor(breakdown, levels=c("WorkStealingComputation", "InitialComputation")))) + 
    geom_bar(stat="identity", position = "stack") +
    labs(x="Tasks", y="Runtime breakdown (seconds)") +
    scale_fill_brewer(palette="Paired", labels=lgLabels, breaks=lgBreaks) +
    facet_wrap(mode ~ descstep, scales="free", ncol=5) +
    theme_minimal() +
    theme(legend.title=element_blank(),
          legend.position="top",
          axis.text.x=element_blank(),
          axis.ticks.x=element_blank(),
          strip.text.y=element_text(angle=0)
          )

ggsave(file="breakdown.pdf", family="serif")
ggsave(file="breakdown.png", family="serif")
