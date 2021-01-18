import pickle


def rw(i):
	with open('edges/' + str(i) + '.p', 'rb') as f:
	    url_path_map_value = pickle.load(f)


	with open('/Users/danielmuraveyko/playground/ir/lab1/spark-2.3.0-bin-hadoop2.7/data/mllib/pagerank_data.txt', 'a') as f: 
	    for key, value in url_path_map_value:
	        f.write(key + " " + value + "\n")



for i in range(12):
	rw(i)