#!/usr/bin/env python

import math


f_truth = open('lr_good_weights.txt', 'r')
f_result = open('ALRresult.log', 'r')

frlines = f_result.readlines()
ftlines = f_truth.readlines()

c = 0
margin = 0.001
for i, line in enumerate(ftlines):
    if math.fabs(float(line) - float(frlines[i])) > margin :
        c = c + 1
print c

f_truth.close()
f_result.close()
