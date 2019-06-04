#!/usr/bin/env python3

# May first need:
# In your VM: sudo apt-get install libgeos-dev (brew install on Mac)
# pip3 install https://github.com/matplotlib/basemap/archive/v1.1.0.tar.gz

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import numpy as np

from mpl_toolkits.basemap import Basemap as Basemap
from matplotlib.colors import rgb2hex
from matplotlib.patches import Polygon

"""
IMPORTANT
This is EXAMPLE code.
There are a few things missing:
1) You may need to play with the colors in the US map.
2) This code assumes you are running in Jupyter Notebook or on your own system.
   If you are using the VM, you will instead need to play with writing the images
   to PNG files with decent margins and sizes.
3) The US map only has code for the Positive case. I leave the negative case to you.
4) Alaska and Hawaii got dropped off the map, but it's late, and I want you to have this
   code. So, if you can fix Hawaii and Alask, ExTrA CrEdIt. The source contains info
   about adding them back.
"""


"""
PLOT 1: SENTIMENT OVER TIME (TIME SERIES PLOT)
"""
# Assumes a file called time_data.csv that has columns
# day, pos_percentage, neg_percentage. Use absolute path.

#TODO order x-axis

# Run this script from the main directory:
# $ python3 PLOT/analysis.py
# We need to standardize the csv filename or keep changing this line every time the csv is generated:
ts = pd.read_csv("/home/cs143/project2/task_10_2.csv/data.csv") #, escapechar='\\'
# Remove erroneous row.
ts = ts[ts['day'] != '2018-12-31']

plt.figure(figsize=(12,5))
ts.day = ts['day']
ts.set_index(['day'],inplace=True)
ts.sort_values('day')

ax = ts.plot(title="President Trump Sentiment on /r/politics Over Time",
        color=['green', 'red'],
       ylim=(0, 1.05))
ax.plot()
plt.savefig("time-plot.png")

"""
PLOT 2: SENTIMENT BY STATE (POSITIVE AND NEGATIVE SEPARATELY)
# This example only shows positive, I will leave negative to you.
"""

# You should use the FULL PATH to the file, just in case.

# We need to standardize the csv filename or keep changing this line every time the csv is generated:
state_data = pd.read_csv("/home/cs143/project2/task_10_3.csv/data.csv")

"""
You also need to download the following files. Put them somewhere convenient:
https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.shp
https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.dbf
https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.shx
IF YOU USE WGET (CONVERT TO CURL IF YOU USE THAT) TO DOWNLOAD THE ABOVE FILES, YOU NEED TO USE 
wget "https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.shp?raw=true"
wget "https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.dbf?raw=true"
wget "https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.shx?raw=true"
The rename the files to get rid of the ?raw=true
"""

# Lambert Conformal map of lower 50 states.
m = Basemap(llcrnrlon=-125, llcrnrlat=15, urcrnrlon=-64, urcrnrlat=49,
        projection='lcc', lat_1=33, lat_2=45, lon_0=-95)
shp_info = m.readshapefile('/home/cs143/project2/PLOT/st99_d00','states',drawbounds=True)  # No extension specified in path here.
pos_data = dict(zip(state_data.state, state_data.pos_percentage))
neg_data = dict(zip(state_data.state, state_data.neg_percentage))
pos_minus_neg_data = dict(zip(state_data.state, state_data.pos_percentage - state_data.neg_percentage))

# choose a color for each state based on sentiment.
pos_colors = {}
neg_colors = {}
pos_minus_neg_colors = {}
statenames = []
pos_cmap = plt.cm.Greens # use 'hot' colormap
neg_cmap = plt.cm.OrRd # use 'hot' colormap
pos_minus_neg_cmap = plt.cm.Blues # use 'hot' colormap

vmin_pos = 0.45; vmax_pos = 0.30 # set range for pos.
vmin_neg = 0.95; vmax_neg = 0.75 # set range for neg.
vmin_pos_minus_neg = -0.7; vmax_pos_minus_neg = -0.25 # set range for neg.
for shapedict in m.states_info:
    statename = shapedict['NAME']
    # skip DC and Puerto Rico.
    if statename not in ['District of Columbia', 'Puerto Rico']:
        pos = pos_data[statename]
        neg = neg_data[statename]
        pos_minus_neg = pos_minus_neg_data[statename]
        # print("pos_minus_neg == %f" % pos_minus_neg)
        # print("State: %s" % statename)
        # print("pos calc: %s" % str(pos_cmap(1. - np.sqrt(( pos - vmin_pos )/( vmax_pos - vmin_pos)))))
        # print("neg calc: %s" % str(neg_cmap(1. - np.sqrt(( neg - vmin_neg )/( vmax_neg - vmin_neg)))))
        pos_colors[statename] = pos_cmap(1. - np.sqrt(( pos - vmin_pos )/( vmax_pos - vmin_pos)))[:3] # 3 for r, g, b?
        neg_colors[statename] = neg_cmap(1. - np.sqrt(( neg - vmin_neg )/( vmax_neg - vmin_neg)))[:3]
        pos_minus_neg_colors[statename] = pos_minus_neg_cmap(1. - np.sqrt(( pos_minus_neg - vmin_pos_minus_neg )/( vmax_pos_minus_neg - vmin_pos_minus_neg)))[:3]
    statenames.append(statename)
# cycle through state names, color each one.

# POSITIVE MAP
ax = plt.gca() # get current axes instance
for nshape, seg in enumerate(m.states):
    # skip Puerto Rico and DC
    if statenames[nshape] not in ['District of Columbia', 'Puerto Rico']:
        # EXTRA CREDIT
        # SOURCE: https://stackoverflow.com/questions/39742305/how-to-use-basemap-python-to-plot-us-with-50-states
        # Move Alaska + Hawaii next to mainland US by changing x,y coords
        if statenames[nshape] == 'Hawaii':
            seg = list(map(lambda args : (args[0] + 5000000, args[1]-1700000), seg))
        if statenames[nshape] == 'Alaska':
        # Shrink Alaska's size to 40% before translating
            seg = list(map(lambda args : (0.40*args[0] + 900000, 0.40*args[1]-1350000), seg))

        color = rgb2hex(pos_colors[statenames[nshape]]) 
        poly = Polygon(seg, facecolor=color, edgecolor=color)
        ax.add_patch(poly)
plt.title('Positive Trump Sentiment Across the US')
plt.savefig("positive-map.png")

# NEGATIVE MAP
ax = plt.gca() # get current axes instance
for nshape, seg in enumerate(m.states):
    # skip Puerto Rico and DC
    if statenames[nshape] not in ['District of Columbia', 'Puerto Rico']:
        # EXTRA CREDIT
        # SOURCE: https://stackoverflow.com/questions/39742305/how-to-use-basemap-python-to-plot-us-with-50-states
        # Move Alaska + Hawaii next to mainland US by changing x,y coords
        if statenames[nshape] == 'Hawaii':
            seg = list(map(lambda args : (args[0] + 5000000, args[1]-1700000), seg))
        if statenames[nshape] == 'Alaska':
        # Shrink Alaska's size to 40% before translating
            seg = list(map(lambda args : (0.40*args[0] + 900000, 0.40*args[1]-1350000), seg))

        color = rgb2hex(neg_colors[statenames[nshape]]) 
        poly = Polygon(seg, facecolor=color, edgecolor=color)
        ax.add_patch(poly)
plt.title('Negative Trump Sentiment Across the US')
plt.savefig("negative-map.png")

# POS MINUS NEG MAP
ax = plt.gca() # get current axes instance
for nshape, seg in enumerate(m.states):
    # skip Puerto Rico and DC
    if statenames[nshape] not in ['District of Columbia', 'Puerto Rico']:
        # EXTRA CREDIT
        # SOURCE: https://stackoverflow.com/questions/39742305/how-to-use-basemap-python-to-plot-us-with-50-states
        # Move Alaska + Hawaii next to mainland US by changing x,y coords
        if statenames[nshape] == 'Hawaii':
            seg = list(map(lambda args : (args[0] + 5000000, args[1]-1700000), seg))
        if statenames[nshape] == 'Alaska':
        # Shrink Alaska's size to 40% before translating
            seg = list(map(lambda args : (0.40*args[0] + 900000, 0.40*args[1]-1350000), seg))
      
        color = rgb2hex(pos_minus_neg_colors[statenames[nshape]]) 
        poly = Polygon(seg, facecolor=color, edgecolor=color)
        ax.add_patch(poly)
plt.title('%Positive - %Negative Trump Sentiment Across the US')
plt.savefig("pos-minus-neg-map.png")


"""
PART 4 SHOULD BE DONE IN SPARK
"""

"""
PLOT 5A: SENTIMENT BY STORY SCORE
"""
# What is the purpose of this? It helps us determine if the story score
# should be a feature in the model. Remember that /r/politics is pretty
# biased.

# Assumes a CSV file called submission_score.csv with the following coluns
# submission_score, pos_percentage, neg_percentage

# We swapped A with B so this one is in directory B
story = pd.read_csv("/home/cs143/project2/task_10_4B.csv/data.csv")
plt.figure(figsize=(12,5))
fig = plt.figure()
ax1 = fig.add_subplot(111)

ax1.scatter(story['submission_score'], story['pos_percentage'], s=10, c='b', marker="s", label='pos_percentage')
ax1.scatter(story['submission_score'], story['neg_percentage'], s=10, c='r', marker="o", label='neg_percentage')
plt.legend(loc='lower right');

plt.xlabel('President Trump Sentiment by Submission Score')
plt.ylabel("Percent Sentiment")
plt.savefig("plot5a.png")

"""
PLOT 5B: SENTIMENT BY COMMENT SCORE
"""
# What is the purpose of this? It helps us determine if the comment score
# should be a feature in the model. Remember that /r/politics is pretty
# biased.

# Assumes a CSV file called comment_score.csv with the following columns
# comment_score, pos_percentage, neg_percentage

# We swapped A with B so this one is in directory A
story = pd.read_csv("/home/cs143/project2/task_10_4A.csv/data.csv")
plt.figure(figsize=(12,5))
fig = plt.figure()
ax1 = fig.add_subplot(111)

ax1.scatter(story['comment_score'], story['pos_percentage'], s=10, c='b', marker="s", label='pos_percentage')
ax1.scatter(story['comment_score'], story['neg_percentage'], s=10, c='r', marker="o", label='neg_percentage')
plt.legend(loc='lower right');

plt.xlabel('President Trump Sentiment by Comment Score')
plt.ylabel("Percent Sentiment")
plt.savefig("plot5b.png")