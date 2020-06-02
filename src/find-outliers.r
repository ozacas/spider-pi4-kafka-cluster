#!/usr/local/bin/R
library(tidyr)
library(ggplot2)
library(dplyr)

t <- as.tbl(read.csv("foo.tsv", header=T, 
                     sep='\t', 
                     na.strings=c("None", "", "?")))
t %>% glimpse()

princomp(t[,3-9], na.action=na.omit, scores=TRUE)$scores
