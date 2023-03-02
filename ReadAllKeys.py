import sys
import getopt
import fdb
import logging
from datetime import datetime
import json

fdb.api_version(710)

def readAllKeys(clusterFile, beginKey, endKey, batchSize, outputFile):
    fdb.options.set_trace_enable()
    db = fdb.open(cluster_file=clusterFile)

    currentBegin = fdb.KeySelector.first_greater_or_equal(beginKey)

    f = None

    if outputFile != "":
        f = open(outputFile, "w")

    total = 0;
    lastKey = None

    while True:
        try:
            tr = db.create_transaction()
            kvs = tr.get_range(currentBegin, endKey, batchSize)
            keyRead = 0
            for key, value in kvs:
                if (outputFile == ""):
                    print (json.dumps({str(fdb.tuple.unpack(key)): str(value)}))
                else:
                    json.dump({str(fdb.tuple.unpack(key)): str(value)}, f)
                total += 1
                keyRead += 1
                if (keyRead == batchSize):
                    lastKey = key
                if total % 10000 == 0:
                    print (total)
            if keyRead < batchSize:
                break
            if total > 30000:
                sys.exit()
            currentBegin = KeySelector.first_greater_than(lastKey)
        except fdb.impl.FDBError as e:
            print ("Get error ", e.code)
            if e.code == 1007:
                batchSize = batchSize / 2
                logging.warning("Getting transaction too old. Shrink batch size to "+str(batchSize))
            else:
                logging.warning("Getting FDB error code "+str(e.code))
    print ("Scan completed. Total row scanned ", total)

def main(argv):
    # logging.basicConfig(filename="read-all-keys.{sdate}.log".format(sdate=datetime.now().strftime("%d-%m-%Y-%H-%M-%S")),
    #                     format='%(asctime)s %(levelname)-4s: %(message)s')
    logging.getLogger().setLevel(logging.INFO)

    helpString = "Usage: ./ReadAllKeys.py -c <cluster file> -b <begin key> -e <end key> -o <output file>"

    clusterFile = ""
    beginKey = b''
    endKey = b'\xff'
    outputFile = ""
    batchSize = 1000

    options = "hc:b:n:o:s:"
    longOptions = ["help", "cluster_file=", "begin=", "end=", "output=", "batch_size="]

    arguments, values = getopt.getopt(argv, options, longOptions)
    for arg, value in arguments:
        if arg in ("-c", "--cluster_file"):
            clusterFile = value
        elif arg in ("-b", "--begin"):
            beginKey = value
        elif arg in ("-e", "--end"):
            endKey = value
        elif arg in ("-o", "--output"):
            outputFile = value
        elif arg in ("-h", "--help"):
            print (helpString)
            sys.exit()

    if clusterFile == "":
        print ("Error. Require cluster file.")
        print (helpString)
        sys.exit()

    logging.info('Reading key range ["{sbegin}":"{send}"] from DB with cluster file "{scluster}". Output path "{soutput}".'.format(sbegin=beginKey, send=endKey, scluster=clusterFile, soutput=("None" if outputFile=="" else outputFile)))

    readAllKeys(clusterFile, beginKey, endKey, batchSize, outputFile)
    

if __name__ == "__main__":
    main(sys.argv[1:])
