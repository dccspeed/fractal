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
base1 <- mean(data$runtime[data$db == "youtube" & data$mindensity == 0.3 & data$nworkers == 1])
base2 <- mean(data$runtime[data$db == "youtube" & data$mindensity == 0.6 & data$nworkers == 1])
base3 <- mean(data$runtime[data$db == "youtube" & data$mindensity == 0.9 & data$nworkers == 1])
data$singlethread[data$db == "youtube" & data$mindensity == 0.3] <- base1 * 28
data$singlethread[data$db == "youtube" & data$mindensity == 0.6] <- base2 * 28
data$singlethread[data$db == "youtube" & data$mindensity == 0.9] <- base3 * 28
data$speedup <- data$singlethread / data$runtime
data$dbmindensity <- paste(data$db, data$mindensity)
print(data)

datac <- summarySE(data, measurevar="speedup", groupvars=c("nworkers", "db", "mindensity", "dbmindensity"))
baseruntime <- datac$runtime[[1]]
basese <- datac$se[[1]]

datac$efficiency <- datac$speedup / (datac$nworkers * 28)

print(datac)

require(ggplot2)
require(scales)

lgLabels <- c("Youtube (vertices = 4, d = 0.30)",
              "Youtube (vertices = 4, d = 0.60)",
              "Youtube (vertices = 4, d = 0.90)")
lgValues <- c("#7570b3", "#1b9e77", "#d95f02")
lgBreaks <- c("youtube 0.3", "youtube 0.6", "youtube 0.9")

func <- function(x) x

plot <- ggplot(datac, aes(x=nworkers*28, y=speedup, colour=dbmindensity)) + 
    stat_function(fun = func, colour="black") +
    geom_line(size=1) +
    geom_errorbar(aes(ymin=(speedup-se)/28, ymax=(speedup+se)/28),
                  colour="black", width=.005) +
    labs(title="", x="Number of cores", y="Speedup") +
    scale_colour_manual(name="", values=lgValues, labels=lgLabels, breaks=lgBreaks) +
    scale_x_continuous(breaks=28*c(1,2,3,4,5,6,7,8,9)) +
    scale_y_continuous(breaks=28*c(1,2,3,4,5,6,7,8,9)) +
    #scale_shape_manual (name="", values=c(22, 23)) +
    #theme_minimal(base_size = 20) +
    theme_minimal() +
    #theme(legend.title=element_blank()) +
    theme(legend.position=c(0.2,0.9))

ggsave(file="quasicliques_youtube_3.pdf", family="serif", heigh=4, width=6)
ggsave(file="quasicliques_youtube_3.png", family="serif", heigh=4, width=6)

ggsave(file="quasicliques_youtube_3_red.pdf", plot + theme_minimal(base_size = 22) + theme(legend.position=c(0.35,0.93)),
       family="serif", heigh=4, width=6)
ggsave(file="quasicliques_youtube_3_red.pdf", plot + theme_minimal(base_size = 22) + theme(legend.position=c(0.35,0.93)),
       family="serif", heigh=4, width=6)
