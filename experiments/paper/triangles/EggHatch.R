# Written as part of an answer for a question posed here:
# https://stackoverflow.com/questions/2895319/how-to-add-texture-to-fill-colors-in-ggplot2/20426482#20426482
# Please add a vote to the original answer linked above if you find this
# function useful.
EggHatch <- function(ggplot2_plot, cond.list, width_Man = NULL){
 
    # use this for Relevel
    library(Epi)
    # default relevel to avoid index issue
    ggplot2_plot$data$Variable = Relevel(ggplot2_plot$data$Variable, ref = c(ggplot2_plot$data$Variable))
 
    print("When choosing patterns ensure that scaling does not obscure horizontal lines. Should horizontal lines be obscured consider a different pattern (A, C, D, or any combination of them.)")
    # if statement for when no value input in 'fill'
      library(stringi)
      library(plyr)
      library(gtools)
      library(stringr)
 
      input_position_dodge<-ggplot2_plot$layers[[1]]$position$width
      input_colour<-ggplot2_plot$layers[[1]]$aes_params$colour
      fill_value<-ggplot2_plot$layers[[1]]$aes_params$fill
      input_size<-ggplot2_plot$theme$rect$size
      input_width<-ggplot2_plot$layers[[1]]$geom_params$width
 
      # remove any spaces, tabs etc. from input string
      cond.list<-stri_replace_all_charclass(cond.list, "\\p{WHITE_SPACE}", "")
 
      # list of conditions input by users
      string_split1<-strsplit(cond.list, ",")[[1]]
 
      # Dataframe shaped to match number of patterns and conditions entries
      df <- data.frame(matrix(ncol = 2, nrow = length(string_split1)))
      colnames(df) <- c("Pattern", "Condition")
 
      pattern_options <- c("A", "B", "C", "D") 
 
      BAR_OUTPUT1<-ggplot2_plot
      BAR_OUTPUT<-BAR_OUTPUT1
 
      for (pos in 1:length(string_split1)){
 
        # seperate pattern from input condition into list, dropping '='
        cond_patterns <- as.list(strsplit(string_split1[pos], "=")[[1]])
 
        # write values to relevant colomns on current row
        df$Pattern[pos]<-cond_patterns[1]
        df$Condition[pos]<-cond_patterns[2]
 
      }
 
      # find largest mean value and pass 2% of it
      two_percent_of_largest_mean<-((max(unlist(lapply(ggplot2_plot$data$Value,FUN=max)))/100)*2)
 
      # loop around for number of conditions
      for (pos in 1:length(df$Condition)){
 
          Condition_Pos=0
 
        # split pattern into individual characters
        pattern_split_loop_var<-as.character(df$Pattern[pos])
        pattern_split_loop_var<- strsplit(pattern_split_loop_var, "")[[1]]
 
        if(grep("A|B|C|D", df$Pattern[pos], ignore.case=TRUE)){
 
          if ((grepl('A', df$Pattern[pos], ignore.case=TRUE))){
 
            # get 25% of original width
            vert_bar_count = (((ggplot2_plot$layers[[1]]$geom_params$width)/100)*10)
            Condition_Pos<-as.numeric(df$Condition[pos])
 
            for (i in 1){
 
              if (is.null(fill_value)){
 
                BAR_OUTPUT<-BAR_OUTPUT+geom_bar(data=BAR_OUTPUT$data[Condition_Pos,], position=position_dodge(input_position_dodge), stat="identity", colour=input_colour, size=input_size, width=(ggplot2_plot$layers[[1]]$geom_params$width))
 
              }
 
              if (!is.null(fill_value)){
 
              BAR_OUTPUT<-BAR_OUTPUT+geom_bar(data=BAR_OUTPUT$data[Condition_Pos,], position=position_dodge(input_position_dodge), stat="identity", colour=input_colour, size=input_size, width=(ggplot2_plot$layers[[1]]$geom_params$width),fill=fill_value)
 
              }
 
              # loop 4 times
              for (i in 1:((ggplot2_plot$layers[[1]]$geom_params$width)/vert_bar_count)){
 
                # when calculating the number of times to loop and draw consider making value = bar width
                # additionally consider add a verticle line '0' by default. Should get around any
                # stepping outside range
                # alternative just draw a line on each unit on the x-axis inside bounds of bar: number at given
                # point (x-axis label) then either side of number, for example:
                # geom_vline(xintercept=c(1.5,2.5, 10.5), linetype="dotted", size=.8)
                #
                # ensure the Condition_Position value is correct here
                BAR_OUTPUT<-BAR_OUTPUT+geom_bar(data=BAR_OUTPUT$data[Condition_Pos,], position=position_dodge(input_position_dodge), stat="identity", colour=input_colour, size=input_size, width=(ggplot2_plot$layers[[1]]$geom_params$width)-vert_bar_count, fill='transparent')
                vert_bar_count = vert_bar_count + (((ggplot2_plot$layers[[1]]$geom_params$width)/100)*10)#((ggplot2_plot$layers[[1]]$geom_params$width)/4)
 
              }
            }
 
            if ((grepl('B', df$Pattern[pos], ignore.case=TRUE))){  
 
              point_1_percent_of_current_mean <- ((BAR_OUTPUT$data$Value[Condition_Pos]/100)*.1)                              
            while((BAR_OUTPUT$data$Value[Condition_Pos])>=two_percent_of_largest_mean){ 
 
                # this breaks while loop to prevent going below x-axis 0
                if(((as.numeric(BAR_OUTPUT$data$Value[Condition_Pos]))-(ggplot2_plot$data$Value[Condition_Pos]/100))<0){
 
                  break
 
                }
 
                BAR_OUTPUT$data$Value[Condition_Pos]<-(as.numeric(BAR_OUTPUT$data$Value[Condition_Pos]))-two_percent_of_largest_mean
 
                # This stops line very close to 0 on y-axis
                if(((BAR_OUTPUT$data$Value[Condition_Pos])<((two_percent_of_largest_mean/100)*15))){
 
                  break
 
                }
 
                BAR_OUTPUT<-BAR_OUTPUT+geom_bar(data=BAR_OUTPUT$data[Condition_Pos,], position=position_dodge(input_position_dodge), stat="identity", colour=input_colour, size=input_size, width = ggplot2_plot$layers[[1]]$geom_params$width, fill='transparent')
 
              }
 
            } # end of sub grepl 'B'
 
            if ((grepl('C', df$Pattern[pos], ignore.case=TRUE))){
 
              # Code for diagonal C
              position_input_x<-as.numeric(df$Condition[pos])
              position_input_y<-as.numeric(df$Condition[pos])
              # half of input width
              # when minused from centre of bar point on x-axis
              # it gives value at start of bar on x-axis
              # inverse true for end of bar width
              half_input_width<-input_width/2
              ten_perc_input_width<-((input_width/100)*10)
 
              x_axis_min <- position_input_x-half_input_width
              x_axis_start_top<-x_axis_min
              x_axis_start_bottom<-x_axis_min
              x_axis_max <- position_input_x+half_input_width
 
              y_axis_max<- ggplot2_plot$data$Value[position_input_y]
              # need to pull the max value from the example_plot the max boundries...
              # or caluclate the
              z_percent_of_current_mean<-(y_axis_max/100)*10
 
              df_diag <- data.frame(
 
                # start on x-axis-end on x-axis
                x = c(x_axis_start_bottom,x_axis_start_top),
 
                # start on y-axis-end on y-axis
                y = c(y_axis_max,y_axis_max), Fill='Hope you\'re having a nice day.')
 
              for(i in 1:19){
 
                # run if 2nd x-position is less or equal to rightmost edge of bar
                # minus 10% of total input width
                if (df_diag$x[2]<(as.character(x_axis_max))){
 
                  # 1/10 of unit value in width field
                  df_diag$x[2]<-df_diag$x[2]+(ten_perc_input_width)
 
                  # subtract z percent from 1st y height
                  df_diag$y[1]<-df_diag$y[1]-(z_percent_of_current_mean)
 
                  # draw using current df_diag values
                  BAR_OUTPUT<-BAR_OUTPUT+geom_path(data=df_diag, aes(x=x, y=y),colour = "black")
 
                } # end of if statement when less than x_axis_max
 
                # run this code to limit 2nd x-point from moving past rightmost edge of bar
                if (df_diag$x[2]==as.character(x_axis_max)){
 
                  # drop height of 2nd y-position by z percentage
                  df_diag$y[2]<-df_diag$y[2]-(z_percent_of_current_mean)
 
                  if(df_diag$y[1]<=z_percent_of_current_mean){
 
                    df_diag$x[1]<-df_diag$x[1]+ten_perc_input_width
                  }
 
                  BAR_OUTPUT<-BAR_OUTPUT+geom_path(data=df_diag, aes(x=x, y=y),colour = "black")
 
                } # end of if statement when x_axis_max reached
 
              } # end of for loop 1:19
 
            } # end of sub grepl 'C'
 
            if ((grepl('D', df$Pattern[pos], ignore.case=TRUE))){
 
              # Code for left leaning diagonal 'D'
              position_input_x<-as.numeric(df$Condition[pos])
              position_input_y<-as.numeric(df$Condition[pos])
              # half of input width
              # when minused from centre of bar point on x-axis
              # it gives value at start of bar on x-axis
              # inverse true for end of bar width
              half_input_width<-input_width/2
              ten_perc_input_width<-((input_width/100)*10)
 
              x_axis_min <- position_input_x-half_input_width
              x_axis_max <- position_input_x+half_input_width
              y_axis_max<- ggplot2_plot$data$Value[position_input_y]
              # need to pull the max value from the example_plot the max boundries...
              # or caluclate the
              z_percent_of_current_mean<-(y_axis_max/100)*10
 
              df_diag <- data.frame( 
                  # start on x-axis-end on x-axis
                  x = c(x_axis_max,x_axis_max),                                              
                  
                  # start on y-axis-end on y-axis              
                  y = c(y_axis_max,y_axis_max), Fill='Hope you\'re having a nice day.')
              for(i in 1:19){                               
                 
                  # run if 1st x-position is less or equal to leftmost edge of bar
                  # minus 10% of total input width 
                  if (df_diag$x[2]>(as.character(x_axis_min))){
 
                  # 1/10 of unit value in width field
                  df_diag$x[2]<-df_diag$x[2]-(ten_perc_input_width)
 
                  # subtract z percent from 1st y height
                  df_diag$y[1]<-df_diag$y[1]-(z_percent_of_current_mean)
 
                  # draw using current df_diag values
                  BAR_OUTPUT<-BAR_OUTPUT+geom_path(data=df_diag, aes(x=x, y=y),colour = "black")
 
                } # end of if statement when less than x_axis_max
 
                # run this code to limit 2nd x-point from moving past rightmost edge of bar
                if (df_diag$x[2]==as.character(x_axis_min)){
 
                  # drop height of 2nd y-position by z percentage
                  df_diag$y[2]<-df_diag$y[2]-(z_percent_of_current_mean)
 
                  if(df_diag$y[1]<=z_percent_of_current_mean){
 
                    df_diag$x[1]<-df_diag$x[1]-ten_perc_input_width
                  }
 
                  BAR_OUTPUT<-BAR_OUTPUT+geom_path(data=df_diag, aes(x=x, y=y),colour = "black")
 
                } # end of if statement when x_axis_max reached
 
              } # end of for loop 1:19
 
            }
 
          } # end of if grepl 'A'
 
          # This runs code when *only* 'B' in string
          if ((!grepl("[^B]", df$Pattern[pos], ignore.case=TRUE))==TRUE){
 
            Condition_Pos<-as.numeric(df$Condition[pos])
            one_percent_horiz<-(BAR_OUTPUT$data$Value[Condition_Pos])/100
            Twenty_percent_horiz<-(one_percent_horiz*20)
 
            point_1_percent_of_current_mean <- ((BAR_OUTPUT$data$Value[Condition_Pos]/100)*.1)                          # draw original bar -> avoids any issue with 'fill' and drawing incorrect height
            BAR_OUTPUT<-BAR_OUTPUT+geom_bar(data=BAR_OUTPUT$data[Condition_Pos,], position=position_dodge(input_position_dodge), stat="identity", colour=input_colour, size=input_size, width = ggplot2_plot$layers[[1]]$geom_params$width)                          
            # drawing lines (via minusing mean) above 0 on y-axis             
            while((BAR_OUTPUT$data$Value[Condition_Pos])>=two_percent_of_largest_mean){ 
 
              # this breaks while loop to prevent going below x-axis 0
              if(((as.numeric(BAR_OUTPUT$data$Value[Condition_Pos]))-(ggplot2_plot$data$Value[Condition_Pos]/100))<0){
 
                break
 
              }
 
              # check null value
              if (is.null(fill_value)){
 
                BAR_OUTPUT$data$Value[Condition_Pos]<-(as.numeric(BAR_OUTPUT$data$Value[Condition_Pos]))-two_percent_of_largest_mean
 
                if(((BAR_OUTPUT$data$Value[Condition_Pos])<((two_percent_of_largest_mean/100)*15))){
 
                  break
 
                }
 
                BAR_OUTPUT<-BAR_OUTPUT+geom_bar(data=BAR_OUTPUT$data[Condition_Pos,], position=position_dodge(input_position_dodge), stat="identity", colour=input_colour, size=input_size, width = ggplot2_plot$layers[[1]]$geom_params$width)
 
              }
 
              if (!is.null(fill_value)){
 
                BAR_OUTPUT$data$Value[Condition_Pos]<-(as.numeric(BAR_OUTPUT$data$Value[Condition_Pos]))-two_percent_of_largest_mean
 
                if(((BAR_OUTPUT$data$Value[Condition_Pos])<((two_percent_of_largest_mean/100)*15))){
 
                  break
 
                }
 
                BAR_OUTPUT<-BAR_OUTPUT+geom_bar(data=BAR_OUTPUT$data[Condition_Pos,], position=position_dodge(input_position_dodge), stat="identity", colour=input_colour, size=input_size, width = ggplot2_plot$layers[[1]]$geom_params$width, fill=fill_value)
 
              }
 
            }
 
          } # end of if grepl 'B'
 
          if ((!grepl('A', df$Pattern[pos], ignore.case=TRUE))==TRUE){
          # Only run this code if 'A' is NOT in string - issue lies in BAR_OUTPUT value
            if ((grepl('C', df$Pattern[pos], ignore.case=TRUE))){
 
              Condition_Pos<-as.numeric(df$Condition[pos])                              
          if ((grepl('B', df$Pattern[pos], ignore.case=TRUE))){                 # draw original bar -> avoids any issue with 'fill' and drawing incorrect height
                BAR_OUTPUT<-BAR_OUTPUT+geom_bar(data=BAR_OUTPUT$data[Condition_Pos,], position=position_dodge(input_position_dodge), stat="identity", colour=input_colour, size=input_size, width = ggplot2_plot$layers[[1]]$geom_params$width)
 
                point_1_percent_of_current_mean <- ((BAR_OUTPUT$data$Value[Condition_Pos]/100)*.1)
                while((BAR_OUTPUT$data$Value[Condition_Pos])>=two_percent_of_largest_mean){
 
                  # this breaks while loop to prevent going below x-axis 0
                  if(((as.numeric(BAR_OUTPUT$data$Value[Condition_Pos]))-(ggplot2_plot$data$Value[Condition_Pos]/100))<0){
 
                    break
 
                  }
 
                  BAR_OUTPUT$data$Value[Condition_Pos]<-(as.numeric(BAR_OUTPUT$data$Value[Condition_Pos]))-two_percent_of_largest_mean
 
                  # This stops line very close to 0 on y-axis
                  if(((BAR_OUTPUT$data$Value[Condition_Pos])<((two_percent_of_largest_mean/100)*15))){
 
                    break
 
                  }
 
                  BAR_OUTPUT<-BAR_OUTPUT+geom_bar(data=BAR_OUTPUT$data[Condition_Pos,], position=position_dodge(input_position_dodge), stat="identity", colour=input_colour, size=input_size, width = ggplot2_plot$layers[[1]]$geom_params$width, fill='transparent')
 
                }
 
              } # end of sub grepl 'B'
 
              # Code for right leaning diagonal 'C'
              position_input_x<-as.numeric(df$Condition[pos])
              position_input_y<-as.numeric(df$Condition[pos])
              # half of input width
              # when minused from centre of bar point on x-axis
              # it gives value at start of bar on x-axis
              # inverse true for end of bar width
              half_input_width<-input_width/2
              ten_perc_input_width<-((input_width/100)*10)
 
              x_axis_min <- position_input_x-half_input_width
              x_axis_max <- position_input_x+half_input_width
 
              y_axis_max<- ggplot2_plot$data$Value[position_input_y]
              # need to pull the max value from the example_plot the max boundries...
              # or caluclate the
              z_percent_of_current_mean<-(y_axis_max/100)*10
 
              df_diag <- data.frame(
 
                                  # start on x-axis-end on x-axis
                                  x = c(x_axis_min,x_axis_min),
 
                                  # start on y-axis-end on y-axis
                                  y = c(y_axis_max,y_axis_max), Fill='Hope you\'re having a nice day.')
 
             for(i in 1:19){
 
                # run if 2nd x-position is less or equal to rightmost edge of bar
                # minus 10% of total input width
                if (df_diag$x[2]<(as.character(x_axis_max))){
 
                  # 1/10 of unit value in width field
                  df_diag$x[2]<-df_diag$x[2]+(ten_perc_input_width)
 
                  # subtract z percent from 1st y height
                  df_diag$y[1]<-df_diag$y[1]-(z_percent_of_current_mean)
 
                  # draw using current df_diag values
                  BAR_OUTPUT<-BAR_OUTPUT+geom_path(data=df_diag, aes(x=x, y=y),colour = "black")
 
                } # end of if statement when less than x_axis_max
 
                # run this code to limit 2nd x-point from moving past rightmost edge of bar
                if (df_diag$x[2]==as.character(x_axis_max)){
 
                  # drop height of 2nd y-position by z percentage
                  df_diag$y[2]<-df_diag$y[2]-(z_percent_of_current_mean)
 
                  if(df_diag$y[1]<=z_percent_of_current_mean){
 
                    df_diag$x[1]<-df_diag$x[1]+ten_perc_input_width
                  }
 
                  BAR_OUTPUT<-BAR_OUTPUT+geom_path(data=df_diag, aes(x=x, y=y),colour = "black")
 
                } # end of if statement when x_axis_max reached
 
              } # end of for loop 1:19
 
            } # end of grepl 'C'
 
            if ((grepl('D', df$Pattern[pos], ignore.case=TRUE))){
 
              Condition_Pos<-as.numeric(df$Condition[pos])                              
            if ((grepl('B', df$Pattern[pos], ignore.case=TRUE))){                                  # draw original bar -> avoids any issue with 'fill' and drawing incorrect height
                BAR_OUTPUT<-BAR_OUTPUT+geom_bar(data=BAR_OUTPUT$data[Condition_Pos,], position=position_dodge(input_position_dodge), stat="identity", colour=input_colour, size=input_size, width = ggplot2_plot$layers[[1]]$geom_params$width)
 
                point_1_percent_of_current_mean <- ((BAR_OUTPUT$data$Value[Condition_Pos]/100)*.1)                                  
                while((BAR_OUTPUT$data$Value[Condition_Pos])>=two_percent_of_largest_mean){
 
                  # this breaks while loop to prevent going below x-axis 0
                  if(((as.numeric(BAR_OUTPUT$data$Value[Condition_Pos]))-(ggplot2_plot$data$Value[Condition_Pos]/100))<0){
 
                    break
 
                  }
 
                  BAR_OUTPUT$data$Value[Condition_Pos]<-(as.numeric(BAR_OUTPUT$data$Value[Condition_Pos]))-two_percent_of_largest_mean
 
                  # This stops line very close to 0 on y-axis
                  if(((BAR_OUTPUT$data$Value[Condition_Pos])<((two_percent_of_largest_mean/100)*15))){
 
                    break
 
                  }
 
                  BAR_OUTPUT<-BAR_OUTPUT+geom_bar(data=BAR_OUTPUT$data[Condition_Pos,], position=position_dodge(input_position_dodge), stat="identity", colour=input_colour, size=input_size, width = ggplot2_plot$layers[[1]]$geom_params$width, fill='transparent')
 
                }
 
              } # end of sub grepl 'B'
 
              # Code for left leaning diagonal 'D'
              position_input_x<-as.numeric(df$Condition[pos])
              position_input_y<-as.numeric(df$Condition[pos])
              # half of input width
              # when minused from centre of bar point on x-axis
              # it gives value at start of bar on x-axis
              # inverse true for end of bar width
              half_input_width<-input_width/2
              ten_perc_input_width<-((input_width/100)*10)
 
              x_axis_min <- position_input_x-half_input_width
              x_axis_max <- position_input_x+half_input_width
 
              y_axis_max<- ggplot2_plot$data$Value[position_input_y]
              # need to pull the max value from the example_plot the max boundries...
              # or caluclate the
              z_percent_of_current_mean<-(y_axis_max/100)*10
 
              df_diag <- data.frame(    
                  # start on x-axis-end on x-axis
                  x = c(x_axis_max,x_axis_max),                                                 
                  # start on y-axis-end on y-axis                 
                  y = c(y_axis_max,y_axis_max), Fill='Hope you\'re having a nice day.')
              for(i in 1:19){
                 # run if 1st x-position is less or equal to leftmost edge of bar               
                 # minus 10% of total input width              
                 if (df_diag$x[2]>(as.character(x_axis_min))){
 
                  # 1/10 of unit value in width field
                  df_diag$x[2]<-df_diag$x[2]-(ten_perc_input_width)
 
                  # subtract z percent from 1st y height
                  df_diag$y[1]<-df_diag$y[1]-(z_percent_of_current_mean)
 
                  # draw using current df_diag values
                  BAR_OUTPUT<-BAR_OUTPUT+geom_path(data=df_diag, aes(x=x, y=y),colour = "black")
 
                } # end of if statement when less than x_axis_max
 
                # run this code to limit 2nd x-point from moving past rightmost edge of bar
                if (df_diag$x[2]==as.character(x_axis_min)){
 
                  # drop height of 2nd y-position by z percentage
                  df_diag$y[2]<-df_diag$y[2]-(z_percent_of_current_mean)
 
                  if(df_diag$y[1]<=z_percent_of_current_mean){
 
                    df_diag$x[1]<-df_diag$x[1]-ten_perc_input_width
                  }
 
                  BAR_OUTPUT<-BAR_OUTPUT+geom_path(data=df_diag, aes(x=x, y=y),colour = "black")
 
                } # end of if statement when x_axis_max reached
 
              } # end of for loop 1:19
 
            } # end of grepl 'D'
 
        } # end of no 'A' if statment
 
        }  
 
        } # second loop
 
      BAR_OUTPUT
 
  }
