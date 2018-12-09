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

base1 <- min(data$runtime[data$db == "mico" & data$size == 3 & data$nworkers == 1])
base2 <- min(data$runtime[data$db == "mico" & data$size == 4 & data$nworkers == 1])

data$singlethread[data$db == "mico" & data$size == 3] <- base1 * 28
data$singlethread[data$db == "mico" & data$size == 4] <- base2 * 28

data$speedup <- data$singlethread / data$runtime
data$dbsize <- paste(data$db, data$size)
print(data)

datac <- summarySE(data, measurevar="speedup", groupvars=c("sys","nworkers", "db", "size", "dbsize"))
baseruntime <- datac$runtime[[1]]
basese <- datac$se[[1]]

require(ggplot2)
require(scales)

lgLabels <- c("Fractal (Mico-SL, 4 vertices)", "Fractal (Mico-SL, 5 vertices)")
cbPalette <- c("#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")
lgBreaks <- c("mico 3", "mico 4")

func <- function(x) x

ggplot(datac, aes(x=nworkers*28, y=speedup, colour=dbsize)) + 
    stat_function(fun = func, colour="black") +
    geom_errorbar(aes(ymin=speedup-se, ymax=speedup+se),
                  colour="black", width=.005) +
    geom_line(size=1) +
    geom_point(aes(shape=dbsize), size=3) +
    labs(title="", x="Number of cores", y="Speedup") +
    scale_colour_manual(name="", values=cbPalette, labels=lgLabels, breaks=lgBreaks) +
    scale_shape_manual(name="", values=c(4,5,6,7), labels=lgLabels, breaks=lgBreaks) +
    scale_x_continuous(breaks=28*c(1,2,3,4,5,6,7,8,9,10)) +
    scale_y_continuous(breaks=28*c(1,2,3,4,5,6,7,8,9,10)) +
    #scale_shape_manual (name="", values=c(22, 23)) +
    theme_classic(base_size = 18) +
    theme(legend.title=element_blank(), legend.position=c(0.36,0.95))

ggsave(file="motifs_scalability.pdf", family="serif", heigh=4, width=6)
ggsave(file="motifs_scalability.png", family="serif", heigh=4, width=6)
