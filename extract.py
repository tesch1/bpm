#!/usr/bin/env python3
import sys
import os
import argparse 
import zipfile
import tempfile
import random
import logging
from datetime import datetime, date
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET


FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('apple_health')
logger.setLevel(logging.DEBUG)


def process_record(args, node, start = None, end = None):
    ''' Process record contained in node, if it is between start and end '''
    if 'type' not in node.attrib:
        #warning?
        return None
    # Extract 1
    # Extract first order HR observations
    # https://developer.apple.com/documentation/healthkit/hkquantitytypeidentifier/2881127-heartratevariabilitysdnn
    
    '''
     <Record type="HKQuantityTypeIdentifierHeartRate" 
         sourceName="Saif Ahmed" 
         sourceVersion="4.0" 
         device="&lt;&lt;HKDevice: 0x282d50a50&gt;, name:Apple Watch, manufacturer:Apple, model:Watch, hardware:Watch3,4, software:4.0&gt;" 
         unit="count/min" 
         creationDate="2017-11-15 00:15:23 -0400" 
         startDate="2017-11-15 00:13:33 -0400" 
         endDate="2017-11-15 00:13:33 -0400" 
         value="76">
     '''    

    # Extract 2
    # Extract HR observations from HR Variability SD
    # https://developer.apple.com/documentation/healthkit/hkquantitytypeidentifier/2881127-heartratevariabilitysdnn

    '''
     <Record type="HKQuantityTypeIdentifierHeartRateVariabilitySDNN" 
         sourceName="Saif Ahmed" 
         sourceVersion="4.1" 
         device="&lt;&lt;HKDevice: 0x282d8eda0&gt;, name:Apple Watch, manufacturer:Apple, model:Watch, hardware:Watch3,4, software:4.1&gt;" 
         unit="ms" 
         creationDate="2017-11-22 19:15:52 -0400" 
         startDate="2017-11-22 19:14:47 -0400" 
         endDate="2017-11-22 19:15:52 -0400" 
         value="32.1111">
      <HeartRateVariabilityMetadataList>
       <InstantaneousBeatsPerMinute bpm="95" time="6:14:48.94 PM"/>
       <InstantaneousBeatsPerMinute bpm="94" time="6:14:49.58 PM"/>
       <InstantaneousBeatsPerMinute bpm="91" time="6:14:50.24 PM"/>
       <InstantaneousBeatsPerMinute bpm="93" time="6:14:50.88 PM"/>
    '''

    # Apply filters
    if 'sourceName' in node.attrib:
        sourceName = node.attrib['sourceName']
    else:
        sourceName = ''
    if 'device' in node.attrib:
        device = node.attrib['device']
    else:
        device = ''
    if args.device and args.device != device:
        return None
    if args.sourceName and args.sourceName != sourceName:
        return None

    st = node.attrib['startDate']
    ed = node.attrib['endDate']
    # filter time range
    if start and end:
        start_ = datetime.strptime(st, '%Y-%m-%d %H:%M:%S %z')
        end_ = datetime.strptime(ed, '%Y-%m-%d %H:%M:%S %z')
        if start < start_ and end < end_:
            return None
        if start > start_ and end > end_:
            return None

    if node.attrib['type']=='HKQuantityTypeIdentifierHeartRate':
        if 'value' in node.attrib:
            bpm = node.attrib['value']
            if not args.summary:
                print(f"{st},{ed},,{bpm}", file=args.outfile)
                #print(f"{st},{bpm},{ed},{sourceName},{device}")

    elif node.attrib['type']=='HKQuantityTypeIdentifierHeartRateVariabilitySDNN':

        seq_st = node.attrib['startDate']
        seq_ed = node.attrib['endDate']
        seq_st_dt = datetime.strptime(seq_st, '%Y-%m-%d %H:%M:%S %z')

        grandchildren = node.iter()
        obs_first = None
        for gc in grandchildren:
            if gc.tag == 'InstantaneousBeatsPerMinute':
                if not obs_first:
                    obs_first = gc.attrib['time']
                    st_dt = datetime.strptime(obs_first, '%H:%M:%S.%f %p')
                            
                if 'bpm' in gc.attrib:
                    bpm = gc.attrib['bpm']

                    # Derive elapsed time offset within observation series
                    tm = gc.attrib['time']
                    ed_dt = datetime.strptime(tm, '%H:%M:%S.%f %p')
                    time_offset = datetime.combine(date.min, ed_dt.time()) - datetime.combine(date.min, st_dt.time())

                    # Apply elapsed time offset to sequence start time
                    seq_st_dt_plusdelta = seq_st_dt+time_offset

                    if not args.summary:
                        print(f"{seq_st_dt_plusdelta},{bpm},{sourceName},{device}", file=args.outfile)
                        #print(f"{st},{ed},{time_offset},{bpm}")
                        #print(f"{seq_st},{seq_ed},{tm},{time_offset},{seq_st_dt_plusdelta},{bpm}")

    return (sourceName, node.attrib['type'])

def process_workout(args, node, root, index):
    st = node.attrib['startDate']
    ed = node.attrib['endDate']
    ty = node.attrib['workoutActivityType']
    start_ = datetime.strptime(st, '%Y-%m-%d %H:%M:%S %z')
    end_ = datetime.strptime(ed, '%Y-%m-%d %H:%M:%S %z')
    print(f'# [{index}] : {ty} : {st} - {ed}', file=args.outfile)
    if not args.summary:
        for child in root.iter():
            #print(node.tag)
            if child.tag == 'Record':
                #print(node.tag)
                process_record(args, child, start_, end_)

def process_data_files(args, exportfile, cdafile):
    logger.info(f"Processing Export File {exportfile}")
    print(file=args.outfile)
    print("start_date,end_date,observation_time,hr_bpm", file=args.outfile)
    tree = ET.parse(exportfile)
    root = tree.getroot()  

    record_types = {}
    workout_idx = 0

    for child in root.iter():
        #print(child.tag)
        if child.tag == 'Record':
            #print(child.tag)
            if not args.workout:
                rt = process_record(args, child)
                if rt in record_types:
                    record_types[rt] = record_types[rt] + 1
                else:
                    record_types[rt] = 1
        elif child.tag == 'Workout':
            workout_idx = workout_idx + 1
            if args.workout and args.workout == workout_idx or args.summary:
                process_workout(args, child, root, workout_idx)

    if args.summary:
        for k,v in record_types.items():
            print(f"# {v:6} Records from sourceName: {k[0]:16} type: {k[1]}", file=args.outfile)
    return True

def prep_and_process_files(args, infile, indir):
    if infile and indir:
        logger.error("Cannot have both an input file to extract and also a ready-extracted data dir")
        raise Exception("Cannot have both an input file to extract and also a ready-extracted data dir")

    if infile:
        if not (os.path.exists(infile)):
            logger.error(f"Bad input file received {infile}")
            raise Exception(f"Bad input file received {infile}")

        input_file = infile
        if (os.path.isdir(infile)):
            # we didnt get an exact file input, lets look for the expected name
            input_file = os.path.join(input_file, "export.zip")

        # is it actually there?
        if not (os.path.exists(input_file)):
            raise Exception(f"Received input directory, but expected input file {input_file} not found")        


        with tempfile.TemporaryDirectory() as tempdir_tarball_contents:

            randomizer = str(random.randint(10000, 99999))
            extract_dir = os.path.join(tempdir_tarball_contents, randomizer)
            os.mkdir(extract_dir)

            logger.info(f"Working in folder {extract_dir}")
            zip_ref = zipfile.ZipFile(input_file, 'r')
            zip_ref.extractall(extract_dir)
            zip_ref.close()

            import glob
            extracted_files = glob.glob(os.path.join(extract_dir, 'apple_health_export', '*'))
            logger.info("Files extracted during preparation:")
            for ef in extracted_files:
                logger.info(f"\tFile: {ef}")


            keyfile1 = os.path.join(extract_dir, "apple_health_export", "export_cda.xml")
            keyfile2 = os.path.join(extract_dir, "apple_health_export", "export.xml")

            if not (os.path.exists(keyfile1)):
                logger.error(f"Bad input file received, missing key file export_cda.xml")            
                raise Exception(f"Bad input file received, missing key file export_cda.xml")

            if not (os.path.exists(keyfile2)):
                logger.error(f"Bad input file received, missing key file export.xml")
                raise Exception(f"Bad input file received, missing key file export.xml")

            return process_data_files(args, keyfile2, keyfile1)


    if indir:
        if not (os.path.exists(indir)):
            logger.error(f"Bad input file received {indir}")            
            raise Exception(f"Bad input file received {indir}")

        if not (os.path.isdir(indir)):
            logger.error(f"Received non-existent input directory {indir}")            
            raise Exception(f"Received non-existent input directory {indir}")        

        keyfile1 = os.path.join(indir, "export_cda.xml")
        keyfile2 = os.path.join(indir, "export.xml")

        if not (os.path.exists(keyfile1)):
            logger.error(f"Bad input file received, missing key file export_cda.xml")            
            raise Exception(f"Bad input file received, missing key file export_cda.xml")

        if not (os.path.exists(keyfile2)):
            logger.error(f"Bad input file received, missing key file export.xml")
            raise Exception(f"Bad input file received, missing key file export.xml")

        return process_data_files(args, keyfile2, keyfile1)

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--outfile', help='Path to output csv, default ./out.csv',
                        type=argparse.FileType('r'))
    parser.add_argument('--infile', help='Path to Apple Health extract zipball')
    parser.add_argument('--datadir', help='Path to Apple Health extracted files')
    parser.add_argument('--device', help='Filter records by device')
    parser.add_argument('--workout', help='Filter records by workout', type=int)
    parser.add_argument('--sourceName', help='Filter records by sourceName')
    parser.add_argument('--summary', help='Just print summary of what is in the export', action='store_true')

    args = parser.parse_args()

    input_file = None 
    data_dir = None 

    if args.infile:
        input_file = args.infile
        logger.info(f"Path to input: {input_file}")

    if args.datadir:
        data_dir = args.datadir
        logger.info(f"Path to readily available data: {data_dir}")

    if not args.outfile:
        if args.summary:
            args.outfile = sys.stdout
        else:
            args.outfile = open('out.csv', 'w')

    prep_and_process_files(args, input_file, data_dir)

if __name__== "__main__":

    headline = r"""
                           _        _    _            _ _   _              
         /\               | |      | |  | |          | | | | |             
        /  \   _ __  _ __ | | ___  | |__| | ___  __ _| | |_| |__           
       / /\ \ | '_ \| '_ \| |/ _ \ |  __  |/ _ \/ _` | | __| '_ \          
      / ____ \| |_) | |_) | |  __/ | |  | |  __| (_| | | |_| | | |         
     /_/___ \_| .__/| .__/|_|\___|_|_|  |_|\___|\__,_|_|\__|_| |_|         
     |  __ \  | | | | |      |  ____|    | |                | |            
     | |  | | |_|_| |_|__ _  | |__  __  _| |_ _ __ __ _  ___| |_ ___  _ __ 
     | |  | |/ _` | __/ _` | |  __| \ \/ | __| '__/ _` |/ __| __/ _ \| '__|
     | |__| | (_| | || (_| | | |____ >  <| |_| | | (_| | (__| || (_) | |   
     |_____/ \__,_|\__\__,_| |______/_/\_\\__|_|  \__,_|\___|\__\___/|_|   
                                                                           
    """   

    #print()
    #print(headline)
    #print("(c) 2019 Saif Ahmed")
    print()                                                                    

    main()
