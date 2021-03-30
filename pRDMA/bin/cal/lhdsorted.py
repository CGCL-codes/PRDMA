import sys
import os
max_mem_used = 0

a = []
ans=['',0.0,0.0,0.0]
with open("output.csv",'w') as output:
    output.write("%s\n"%('label'+','+str(1899)+','+str(1989)+','+'average'))
    for root, dirs, files in os.walk(".", topdown=False):
        for i in dirs:
            pathnow = './' + i + '/'
            for rootnow, dirnow, filenow in os.walk(pathnow):
                for filename in filenow:
                    tracepath = pathnow + filename
                    label_temp = filename.split('.')[0].split('_')
                    label = i+'_'+label_temp[1]+'_'+label_temp[2]
                    total = 0
                    with open(tracepath) as tracefile:
                        for line in tracefile:
                            total = total + float(line)
                            a.append(float(line))
                        b = sorted(a)
                        output.write("%s\n"%(label+','+str(b[28500])+','+str(b[29700])+','+str(total/(len(a)))))
                        a = []
