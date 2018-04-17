import codecs

file = codecs.open('keywords.txt', 'r', 'utf-8')

output = codecs.open('twitter_crawler/queries.txt', 'w', 'utf-8')

lines = [line.strip() for line in file]
# since = "2018-03-01"
queries = []
for line in lines:
    determiners, phases = line.split(':')
    for phase in phases.split(','):
        queries.append(phase + ' ' + ' OR '.join(determiners.split(',')))
            # + ' since:' + since)

output.write(','.join(queries))