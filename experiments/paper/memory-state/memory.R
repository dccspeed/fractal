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
data <- read.table(header=T, file="memory.dat")
datac <- summarySE(data, measurevar="mem", groupvars=c("sys", "alg", "graph", "depth"))

datac <- datac[datac$sys == "fractal",]

print(datac)

require(ggplot2)
require(scales)

lgLabels <- c("Arabesque", "Fractal")
cbPalette <- c("#0072B2","#CC79A7")
lgBreaks <- c("arabesque", "fractal")

ggplot(datac, aes(x=depth, y=mem, group=alg)) +
  geom_line(aes(color=alg), size=1) +
  geom_errorbar(aes(ymin=mem-se, ymax=mem+se), colour="black", width=.005) +
  scale_color_brewer(palette="Spectral") +
  theme_classic(base_size = 18) +
  theme(legend.title=element_blank(), legend.position=c(0.36,0.95)) +
  labs(x="Depth", y="Memory Used (GB)")

ggsave(file="fractal-memory.pdf", family="serif", heigh=4, width=6)
ggsave(file="fractal-memory.png", family="serif", heigh=4, width=6)
