import sys

for line in sys.stdin:
        line = line.strip()
        station_id = line[4:15] 
        year = line[15:19]
        month = line[19:21]
        day = line[21:23]
        temp = line[87:92]
        quality = line[92:93] 
        print '\t'.join([station_id, year, month, day, temp, quality])
