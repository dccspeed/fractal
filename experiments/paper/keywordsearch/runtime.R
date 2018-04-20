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
datac <- summarySE(data, measurevar="runtime", groupvars=c("query", "mode", "nworkers"))

datac$group <- paste(datac$query, datac$mode)

print(datac)

require(ggplot2)
require(scales)

lgLabels <- c(expression(G), expression(G["0"]))
#lgLabels <- c(expression(Q[1]~"+"~G), expression(Q[1]~"+"~G["red"]),
#              expression(Q[2]~"+"~G), expression(Q[2]~"+"~G["red"]),
#              expression(Q[3]~"+"~G["red"]))
lgValues <- c("#7570b3", "#1b9e77", "#d95f02")
lgBreaks <- c("1ngr", "2gr")
#lgBreaks <- c("q1 1ngr", "q1 2gr", "q2 3ngr", "q2 4gr",
#              "q3 6gr")

query_names <- list(
  'q1'=expression(Q[1]),
  'q2'=expression(Q[2]),
  'q3'=expression(Q[3]),
  'q4'=expression(Q[4])
)

query_labeller <- function(variable,value){
  return(query_names[value])
}

ggplot(datac, aes(x=factor(nworkers * 28), y=runtime/1000, fill=factor(mode))) + 
    geom_bar(position=position_dodge(), size=10, stat="identity") +
    geom_errorbar(aes(ymin=(runtime-se)/1000, ymax=(runtime+se)/1000),
                  colour="black", width=.005,
                  position=position_dodge(.9)) +
    labs(x="Number of cores", y="Runtime (seconds)") +
    #scale_fill_manual(values=lgValues, labels=lgLabels, breaks=lgBreaks) +
    scale_fill_brewer(palette="Paired", labels=lgLabels, breaks=lgBreaks) +
    #scale_y_log10() +
    theme_minimal(base_size = 18) +
    theme(legend.title=element_blank(), #legend.position=c(0.9,0.9),
          legend.text.align=0, strip.text.y = element_text(angle = 0)) +
    facet_grid(query~., labeller=query_labeller, scales="free_y")

ggsave(file="graph_reduction_runtime.pdf", family="serif", heigh=4, width=6)
ggsave(file="graph_reduction_runtime.png", family="serif", heigh=4, width=6)
