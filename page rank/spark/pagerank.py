
import re
import sys
from operator import add
from pyspark import SparkContext

def parseLinks(urls):
    parts = re.split(r'\t', urls)
    return parts[0], parts[1]

#regex toget data from raw input line and parse in required manner
def ParseLine(s):
	data = []
	match = re.match(r'.*<title>(.*?)</title>',s)
	title = match.group(1)
	
	if re.match(r'.*<text(.*?)</text>',s) is not None:
		text = re.match(r'.*<text(.*?)</text>',s).group(1)
		links = re.findall(r'\[\[(.*?)\]\]',text)
		
		for link in links:
			data.append(title+"\t"+link) 
#return data in as [a\tlink1,a\tlink2....b\tlink1...]			
	return data
def calculate_rank(links,rank):
#recives list of links and the rank of page which pointed to it.we send back partision rank of each link
	links_length = len(links)
	for link in links:
		yield(link, rank/links_length)
	
# if the input arguments are less than 4 then, you exection stops 
if __name__ == "__main__":
	if len(sys.argv) != 4:
	        print >> sys.stderr, "Usage: pagerank <input_file> <input_file> <iterations>"
	        exit(-1)
#reading input from the path mentioned and storing it  a lines rdd										   
	sc = SparkContext(appName="PythonPageRank")
	lines = sc.textFile(sys.argv[1], 1)
# if at all some input files have empty lines are present ,we are deleting them from our rdd, to calulate initial rank and avoid further exceptions.
	filtered = lines.filter(lambda x: not re.match(r'^\s*$', x))
	initial_rank = 1.0/lines.count()
#parse each line to get data in required format and leave lines if they are ill formatted
	links = filtered.flatMap(lambda line: ParseLine(line))
	seperatedLinks= links.map(lambda x: parseLinks(x)) 
#group data by key,so that all links page forwards to are stored in array of links
	links = seperatedLinks.groupByKey()
	links.collect()
#assign  initial rank to 1/(total pages in corpse) and add it to links rdd
	rank = links.map(lambda page: (page[0], initial_rank))
	links.join(rank) 
# take third argument and loop over for updated rank 
	for x in xrange(int(sys.argv[3])):
		individual_rank = links.join(rank).flatMap(lambda x : calculate_rank(x[1][0],x[1][1]))
		rank = individual_rank.reduceByKey(lambda x,y: 0.15+(x+y)*0.85)
# Store sorted data in specified outpath
	rank = rank.sortBy(lambda x: x[1], ascending=False, numPartitions=None)
	rank.saveAsTextFile(sys.argv[2])
	sc.stop()
