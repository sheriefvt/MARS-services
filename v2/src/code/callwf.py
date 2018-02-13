__author__ = 'Sherif'

import requests, json
import time
import matplotlib.pyplot as plt
workflow_path = 'C:/Users/Sherif/WaveMaker/StudioDesktop/7.7.0/studioWorkSpace/workspace/default/projects/MARS/src/main/webapp/resources/images/plots/'



cnames = {
'aliceblue':            '#F0F8FF',
'antiquewhite':         '#FAEBD7',
'aqua':                 '#00FFFF',
'aquamarine':           '#7FFFD4',
'azure':                '#F0FFFF',
'beige':                '#F5F5DC',
'bisque':               '#FFE4C4',
'black':                '#000000',
'blanchedalmond':       '#FFEBCD',
'blue':                 '#0000FF',
'blueviolet':           '#8A2BE2',
'brown':                '#A52A2A',
'burlywood':            '#DEB887',
'cadetblue':            '#5F9EA0',
'chartreuse':           '#7FFF00',
'chocolate':            '#D2691E',
'coral':                '#FF7F50',
'cornflowerblue':       '#6495ED',
'cornsilk':             '#FFF8DC',
'crimson':              '#DC143C',
'cyan':                 '#00FFFF',
'darkblue':             '#00008B',
'darkcyan':             '#008B8B',
'darkgoldenrod':        '#B8860B',
'darkgray':             '#A9A9A9',
'darkgreen':            '#006400',
'darkkhaki':            '#BDB76B',
'darkmagenta':          '#8B008B',
'darkolivegreen':       '#556B2F',
'darkorange':           '#FF8C00',
'darkorchid':           '#9932CC',
'darkred':              '#8B0000',
'darksalmon':           '#E9967A',
'darkseagreen':         '#8FBC8F',
'darkslateblue':        '#483D8B',
'darkslategray':        '#2F4F4F',
'darkturquoise':        '#00CED1',
'darkviolet':           '#9400D3',
'deeppink':             '#FF1493',
'deepskyblue':          '#00BFFF',
'dimgray':              '#696969',
'dodgerblue':           '#1E90FF',
'firebrick':            '#B22222',
'floralwhite':          '#FFFAF0',
'forestgreen':          '#228B22',
'fuchsia':              '#FF00FF',
'gainsboro':            '#DCDCDC',
'ghostwhite':           '#F8F8FF',
'gold':                 '#FFD700',
'goldenrod':            '#DAA520',
'gray':                 '#808080',
'green':                '#008000',
'greenyellow':          '#ADFF2F',
'honeydew':             '#F0FFF0',
'hotpink':              '#FF69B4',
'indianred':            '#CD5C5C',
'indigo':               '#4B0082',
'ivory':                '#FFFFF0',
'khaki':                '#F0E68C',
'lavender':             '#E6E6FA',
'lavenderblush':        '#FFF0F5',
'lawngreen':            '#7CFC00',
'lemonchiffon':         '#FFFACD',
'lightblue':            '#ADD8E6',
'lightcoral':           '#F08080',
'lightcyan':            '#E0FFFF',
'lightgoldenrodyellow': '#FAFAD2',
'lightgreen':           '#90EE90',
'lightgray':            '#D3D3D3',
'lightpink':            '#FFB6C1',
'lightsalmon':          '#FFA07A',
'lightseagreen':        '#20B2AA',
'lightskyblue':         '#87CEFA',
'lightslategray':       '#778899',
'lightsteelblue':       '#B0C4DE',
'lightyellow':          '#FFFFE0',
'lime':                 '#00FF00',
'limegreen':            '#32CD32',
'linen':                '#FAF0E6',
'magenta':              '#FF00FF',
'maroon':               '#800000',
'mediumaquamarine':     '#66CDAA',
'mediumblue':           '#0000CD',
'mediumorchid':         '#BA55D3',
'mediumpurple':         '#9370DB',
'mediumseagreen':       '#3CB371',
'mediumslateblue':      '#7B68EE',
'mediumspringgreen':    '#00FA9A',
'mediumturquoise':      '#48D1CC',
'mediumvioletred':      '#C71585',
'midnightblue':         '#191970',
'mintcream':            '#F5FFFA',
'mistyrose':            '#FFE4E1',
'moccasin':             '#FFE4B5',
'navajowhite':          '#FFDEAD',
'navy':                 '#000080',
'oldlace':              '#FDF5E6',
'olive':                '#808000',
'olivedrab':            '#6B8E23',
'orange':               '#FFA500',
'orangered':            '#FF4500',
'orchid':               '#DA70D6',
'palegoldenrod':        '#EEE8AA',
'palegreen':            '#98FB98',
'paleturquoise':        '#AFEEEE',
'palevioletred':        '#DB7093',
'papayawhip':           '#FFEFD5',
'peachpuff':            '#FFDAB9',
'peru':                 '#CD853F',
'pink':                 '#FFC0CB',
'plum':                 '#DDA0DD',
'powderblue':           '#B0E0E6',
'purple':               '#800080',
'red':                  '#FF0000',
'rosybrown':            '#BC8F8F',
'royalblue':            '#4169E1',
'saddlebrown':          '#8B4513',
'salmon':               '#FA8072',
'sandybrown':           '#FAA460',
'seagreen':             '#2E8B57',
'seashell':             '#FFF5EE',
'sienna':               '#A0522D',
'silver':               '#C0C0C0',
'skyblue':              '#87CEEB',
'slateblue':            '#6A5ACD',
'slategray':            '#708090',
'snow':                 '#FFFAFA',
'springgreen':          '#00FF7F',
'steelblue':            '#4682B4',
'tan':                  '#D2B48C',
'teal':                 '#008080',
'thistle':              '#D8BFD8',
'tomato':               '#FF6347',
'turquoise':            '#40E0D0',
'violet':               '#EE82EE',
'wheat':                '#F5DEB3',
'white':                '#FFFFFF',
'whitesmoke':           '#F5F5F5',
'yellow':               '#FFFF00',
'yellowgreen':          '#9ACD32'}

urlworkflow2 = "http://edison.vbi.vt.edu:9002/workflowservice/execute"
#
urlworkflow2 = "http://localhost:8081/workflowservice/execute"
datawf1 = {'wf':"start,network_data,count_data,save_data,network_data,"
                "count_data,save_data,network_data,count_data,save_data,"
                "network_data,count_data,save_data,network_data,count_data,"
                "save_data,network_data,count_data,save_data,network_data,count_data,save_data,"
                "network_data,count_data,save_data,network_data,count_data,save_data,"
                "plot_data,end",'input':"id,liberia,node,age >=0 and "
                "age <=10|0-10|id,liberia,node,age >=11 and age <=20|"
                "11-20|id,liberia,node,age >=21 and age <=30|21-30|id,liberia,node,age >=31 "
                "and age <=40|31-40|id,liberia,node,age >=41 and age <=50|41-50|id,liberia,node,age >=51 and age <=60|51-60|"
                "id,liberia,node,age >=61 and age <=70|61-70|id,liberia,node,age >=71 and age <=80|71-80|"
                "id,liberia,node,age >=81 and age <=90|81-90|3,0-10:11-20:21-30:31-40:41-50:51-60:61-70:71-80:81-90,g:g:g:g:g:g:g:g:g,Number of Persons,Age Range,30,30,Age Distribution"}



datawf2new = {'wf':"start,network_data,sum_data,save_data,network_data,"
                "sum_data,save_data,network_data,sum_data,save_data,"
                "network_data,sum_data,save_data,network_data,sum_data,"
                "save_data,network_data,sum_data,save_data,network_data,sum_data,save_data,"
                "network_data,sum_data,save_data,network_data,sum_data,save_data,"
                "plot_data,end",'input':"degree,liberia,node,age >=0 and "
                "age <=10|0-10|degree,liberia,node,age >=11 and age <=20|"
                "11-20|degree,liberia,node,age >=21 and age <=30|21-30|degree,liberia,node,age >=31 "
                "and age <=40|31-40|degree,liberia,node,age >=41 and age <=50|41-50|degree,liberia,node,age >=51 and age <=60|51-60|"
                "degree,liberia,node,age >=61 and age <=70|61-70|degree,liberia,node,age >=71 and age <=80|71-80|"
                "degree,liberia,node,age >=81 and age <=90|81-90|3,0-10:11-20:21-30:31-40:41-50:51-60:61-70:71-80:81-90,y:y:y:y:y:y:y:y:y,Sum of Degrees,Age Range,30,30,Age Distribution"}

datawf2new1 = {'wf':"start,network_data,average_data,save_data,network_data,"
                "average_data,save_data,network_data,average_data,save_data,"
                "network_data,average_data,save_data,network_data,average_data,"
                "save_data,network_data,average_data,save_data,network_data,average_data,save_data,"
                "network_data,average_data,save_data,network_data,average_data,save_data,"
                "plot_data,end",'input':"degree,liberia,node,age >=0 and "
                "age <=10|0-10|degree,liberia,node,age >=11 and age <=20|"
                "11-20|degree,liberia,node,age >=21 and age <=30|21-30|degree,liberia,node,age >=31 "
                "and age <=40|31-40|degree,liberia,node,age >=41 and age <=50|41-50|degree,liberia,node,age >=51 and age <=60|51-60|"
                "degree,liberia,node,age >=61 and age <=70|61-70|degree,liberia,node,age >=71 and age <=80|71-80|"
                "degree,liberia,node,age >=81 and age <=90|81-90|3,0-10:11-20:21-30:31-40:41-50:51-60:61-70:71-80:81-90,brown:brown:brown:brown:brown:brown:brown:brown:brown,Average of Degrees,Age Range,30,30,Age Distribution"}


datawfg = {'wf':"start,network_data,count_data,save_data,network_data,count_data,save_data,network_data,count_data,save_data,plot_data,end",
           'input':'id,liberia,node,age >=31 and age<=40|All|id,liberia,node, age >=31 and age<=40 and gender = 1|Male|id,liberia,node, age >=31 and age<=40 and gender = 2|Female|'
                   '3,All:Male:Female,goldenrod:g:c,Number,Gender,30,30,Gender Distribution'}


datawf2 = {'wf':"start,network_data,min_data,save_data,network_data,min_data,save_data,network_data,min_data,save_data,plot_data,end",
           'input':'degree,liberia,node,age >=21 and age <=30|21-30|degree,liberia,node,age >=31 and age<=40|31-40|degree,liberia,node,age >=41 and age<=50|41-50|'
                   '3,21-30:31-40:41-50,#624ea7:yellow:maroon,Minimum Degree,Age Range,25,30, Min. Degree in Diff. Age Groups'}

#(varnames,vavalues,colors,figid,yl,xl,xfontsize,tfontsize,title)

datawf2d = {'wf':"start,network_data,plot_data,end",'input':"degree,er1,node,no condition|1,Frequency,Degree,30,30,goldenrod,o,10,Degree Distribution,_,5"}

datawf2 = {'wf':"start,network_data,plot_data,end",'input':"clustering,slashdot,node,no condition|2,Frequency,Clustering Coefficient,25,20,g,o,15,Clustering Coefficient Distribution,20,-,5"}

datawf23 = {'wf':"start,network_data,plot_data,end",'input':"core,sf1,node,no condition|4,==1:==2:==3:==4:==5:==6:==7,Core 1:Core 2:Core 3:Core 4:Core 5:Core 6:Core 7,r:goldenrod:g:b:y:c:orange,15,Percentage of Nodes at Cores from 1 to 7"}

datawf2 = {'wf':"start,network_data,average_data,end",'input':"clustering,enron,node,no condition"}

datawf2 = {'wf':"start,network_data,plot_data,end",'input':"household,liberia,node,no condition|1,Frequency,Household,25,30,goldenrod,o,10,Household Distribution,_,5"}

datawfc = {'wf':"start,network_data,plot_data,end",'input':"community,wikivote,node,no condition|4,==0:==1:==2:==3:==4,Community 0:Community 1:Community 2:Community 3:Community 4,deeppink:b:lime:y:yellow,25,Nodes in Communities from 0 to 4"}
datawfk = {'wf':"start,network_data,plot_data,end",'input':"core,liberia,node,no condition|1,Frequency,k-shell,30,30,blueviolet,s,10,k-shell Distribution,-,5"}

#datawf2 = {'wf':"start,network_data,end",'input':"clustering,slashdot,node,no condition"}

import wf_library
datawfq = {'wf':"start,execute_query,end",'input':"select edges from liberia where u.core = 3 and v.core = 4 and u.age >=0 and u.age<10 and v.age>=0 and v.age<10,liberia,edge"}

r = requests.get(urlworkflow2,params=datawfg)

# url = "http://edisondev.vbi.vt.edu:9000/graphservice/query"
# data = {'content':'select nodes from karate','graph':'karate','validate':'False','view':'property','parallel':'False','nruns':4,'memo':'True','details':True}
# r = requests.get(url,params=data)

print r.text

################begin multi plot clustering
# datawf2 = {'wf':"start,network_data,end",'input':"clustering,enron,node,no condition"}
# r = requests.get(urlworkflow2,params=datawf2)
# z=json.loads(r.text)
#
# wf_library.plot_clustering_distribution(z.get('datavalue')[0],"ss",20,"Frequency","Clustering Coefficient",30,30,'olive','o',15,'-',5,'')
#
#
# datawf2 = {'wf':"start,network_data,end",'input':"clustering,epinions,node,no condition"}
# r = requests.get(urlworkflow2,params=datawf2)
#
# z=json.loads(r.text)
#
# wf_library.plot_clustering_distribution(z.get('datavalue')[0],"ss",20,"Frequency","Clustering Coefficient",30,30,'orange','*',15,'-',5,'')
#
#
# datawf2 = {'wf':"start,network_data,end",'input':"clustering,slashdot,node,no condition"}
# r = requests.get(urlworkflow2,params=datawf2)
#
# z=json.loads(r.text)
#
# wf_library.plot_clustering_distribution(z.get('datavalue')[0],"ss",20,"Frequency","Clustering Coefficient",30,30,'brown','^',15,'-',5,'')
#
#
# plt.legend(['Enron','Epinions','Slashdot'], loc='upper right',prop={'size':25})
# plt.savefig (workflow_path+"{fi}.png".format(fi="ss"),bbox_inches='tight')
# plt.savefig (workflow_path+"{fi}.pdf".format(fi="ss"),bbox_inches='tight')
# plt.close()

###############begin multi plot degree

# datawf2 = {'wf':"start,network_data,end",'input':"degree,enron,node,no condition"}
# r = requests.get(urlworkflow2,params=datawf2)
# z=json.loads(r.text)
#
# wf_library.plot_degree_distribution(z.get('datavalue')[0],"ssd","Frequency","Degree",25,30,'olive','o',10,'_',5,'Degree Distribution')
#
#
# datawf2 = {'wf':"start,network_data,end",'input':"degree,epinions,node,no condition"}
# r = requests.get(urlworkflow2,params=datawf2)
#
# z=json.loads(r.text)
#
# wf_library.plot_degree_distribution(z.get('datavalue')[0],"ssd","Frequency","Degree",25,30,'orange','o',10,'_',5,'Degree Distribution')
#
#
# datawf2 = {'wf':"start,network_data,end",'input':"degree,slashdot,node,no condition"}
# r = requests.get(urlworkflow2,params=datawf2)
#
# z=json.loads(r.text)
#
# wf_library.plot_degree_distribution(z.get('datavalue')[0],"ssd","Frequency","Degree",25,30,'brown','o',10,'_',5,'Degree Distribution')
#
#
# plt.legend(['Enron','Epinions','Slashdot'], loc='upper right',prop={'size':25},numpoints=1)
# plt.savefig (workflow_path+"{fi}.png".format(fi="ssd"),bbox_inches='tight')
# plt.savefig (workflow_path+"{fi}.pdf".format(fi="ssd"),bbox_inches='tight')
# plt.close()


##plot k-shell for different age bins


# datawf2 = {'wf':"start,network_data,end",'input':"core,liberia,node,age >=0 and age <=10"}
# r = requests.get(urlworkflow2,params=datawf2)
# z=json.loads(r.text)
#
# wf_library.plot_degree_distribution(z.get('datavalue')[0],"ssd","Frequency","k-shell",30,30,'blueviolet','s',10,'-',5,'k-shell Distribution')
#
#
# datawf2 = {'wf':"start,network_data,end",'input':"core,liberia,node,age >=11 and age <=20"}
# r = requests.get(urlworkflow2,params=datawf2)
# z=json.loads(r.text)
#
# wf_library.plot_degree_distribution(z.get('datavalue')[0],"ssd","Frequency","k-shell",30,30,'magenta','s',10,'-',5,'k-shell Distribution')

# datawf2 = {'wf':"start,network_data,end",'input':"core,liberia,node,age >=21 and age <=30"}
# r = requests.get(urlworkflow2,params=datawf2)
# z=json.loads(r.text)
#
# wf_library.plot_degree_distribution(z.get('datavalue')[0],"ssd","Frequency","k-shell",30,30,'maroon','s',10,'-',5,'k-shell Distribution')
#
# datawf2 = {'wf':"start,network_data,end",'input':"core,liberia,node,age >=31 and age <=40"}
# r = requests.get(urlworkflow2,params=datawf2)
# z=json.loads(r.text)
#
# wf_library.plot_degree_distribution(z.get('datavalue')[0],"ssd","Frequency","k-shell",30,30,'green','s',10,'-',5,'k-shell Distribution')
#
# datawf2 = {'wf':"start,network_data,end",'input':"core,liberia,node,age >=41 and age <=50"}
# r = requests.get(urlworkflow2,params=datawf2)
# z=json.loads(r.text)
#
# wf_library.plot_degree_distribution(z.get('datavalue')[0],"ssd","Frequency","k-shell",30,30,'goldenrod','s',10,'-',5,'k-shell Distribution')
#
# datawf2 = {'wf':"start,network_data,end",'input':"core,liberia,node,age >=51 and age <=60"}
# r = requests.get(urlworkflow2,params=datawf2)
# z=json.loads(r.text)
#
# wf_library.plot_degree_distribution(z.get('datavalue')[0],"ssd","Frequency","k-shell",30,30,'brown','s',10,'-',5,'k-shell Distribution')
#

###

# datawf2 = {'wf':"start,network_data,end",'input':"core,liberia,node,age >=61 and age <=70"}
# r = requests.get(urlworkflow2,params=datawf2)
# z=json.loads(r.text)
#
# wf_library.plot_degree_distribution(z.get('datavalue')[0],"ssd","Frequency","k-shell",30,30,'blue','s',10,'-',5,'k-shell Distribution')
#
#
# datawf2 = {'wf':"start,network_data,end",'input':"core,liberia,node,age >=71 and age <=80"}
# r = requests.get(urlworkflow2,params=datawf2)
# z=json.loads(r.text)
#
# wf_library.plot_degree_distribution(z.get('datavalue')[0],"ssd","Frequency","k-shell",30,30,'tomato','s',10,'-',5,'k-shell Distribution')
#
# datawf2 = {'wf':"start,network_data,end",'input':"core,liberia,node,age >=81 and age <=90"}
# r = requests.get(urlworkflow2,params=datawf2)
# z=json.loads(r.text)
#
# wf_library.plot_degree_distribution(z.get('datavalue')[0],"ssd","Frequency","k-shell",30,30,'yellow','s',10,'-',5,'k-shell Distribution')
#
#
#
# plt.legend(['0-11','11-20','61-70','71-80','81-90'], loc='upper right',prop={'size':25},numpoints=1,borderpad=0)
# plt.savefig (workflow_path+"{fi}.png".format(fi="ssd"),bbox_inches='tight')
# plt.savefig (workflow_path+"{fi}.pdf".format(fi="ssd"),bbox_inches='tight')
# plt.close()
