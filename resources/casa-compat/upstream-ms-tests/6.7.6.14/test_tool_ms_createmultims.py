##########################################################################
# test_tool_ms_createmultims.py
#
# Copyright (C) 2018
# Associated Universities, Inc. Washington DC, USA.
#
# This script is free software; you can redistribute it and/or modify it
# under the terms of the GNU Library General Public License as published by
# the Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
#
# This library is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Library General Public
# License for more details.
#
# Based on the requirements listed in casadocs found here:
# https://casadocs.readthedocs.io/en/stable/api/tt/casatools.ms.html#casatools.ms.ms.createmultims
#
#
##########################################################################
import os
import sys
import shutil
import glob
import unittest
import numpy as np

from casatools import ms
from casatools import table
from casatools import ctsys

myname = 'test_tool_ms_createmultims'

# name of the resulting MS
msname = 'multims.ms'


###
###     listpartition(...) was imported from task_listpartition.py
###          axisType(...) was imported from partitionhelper.py
###      getDiskUsage(...) was imported from partitionhelper.py
###          getWidth(...) was imported from task_listparition.py
### getScanSpwSummary(...) was imported from partitionhelper.py
###
def listpartition(vis=None, createdict=None, listfile=None):

    """List information about an MMS data set in the logger:

       Keyword arguments:
       vis -- Name of multi-MS or normal MS.
               default: none. example: vis='uidA002.mms'
       createdict -- Create and return a dictionary with information about
                     the sub-MSs.
               default: False
       listfile -- save the output to a file
             default: ''. Example: listfile="mylist.txt"

        The task also returns a dictionary with scan summary information
        for each sub-MS.


       """
    def axisType(mmsname):
        """Get the axisType information from a Multi-MS. The AxisType information
           is usually added for Multi-MS with the axis which data is parallelized across.

           Keyword arguments:
               mmsname    --    name of the Multi-MS

            It returns the value of AxisType or an empty string if it doesn't exist.
        """
        tbt_local = table( )

        axis = ''

        try:
            tbt_local.open(mmsname, nomodify=True)
            tbinfo = tbt_local.info()
        except Exception as exc:
            raise ValueError("Unable to open table {0}. Exception: {1}".format(mmsname, exc))
        finally:
            tbt_local.close()

        if 'readme' in tbinfo:
            readme = tbinfo['readme']
            readlist = readme.splitlines()
            for val in readlist:
                if val.__contains__('AxisType'):
                    a,b,axis = val.partition('=')

        return axis.strip()

    def getDiskUsage(msfile):
        """Return the size in bytes of an MS or MMS in disk.

        Keyword arguments:
           msfile  --> name of the MS
           This function will return a value given by the command du -hs
        """

        from subprocess import Popen, PIPE, STDOUT

        # Command line to run
        ducmd = 'du -hs {0}'.format(msfile)

        p = Popen(ducmd, shell=True, stdin=None, stdout=PIPE, stderr=STDOUT, close_fds=True)
        o, e = p.communicate()             ### previously 'sizeline = p.stdout.read()' here
                                           ### left process running...
        sizeline = o.decode().split( )[0]

        # Create a list of the output string, which looks like this:
        # ' 75M\tuidScan23.data/uidScan23.0000.ms\n'
        # This will create a list with [size,sub-ms]
        mssize = sizeline.split()

        return mssize[0]

    def getWidth(adict, par):
        width = 0
        for aa in adict.keys():
            scans = adict[aa]['scanId'].keys()
            for bb in scans:
                if par == 'spw':
                    spws = adict[aa]['scanId'][bb]['spwIds']
                    mystr = str(spws)
                    length = len(mystr)
                    if length > width:
                        width = length

                elif par == 'channel':
                    chans = adict[aa]['scanId'][bb]['nchans']
                    mystr = str(chans)
                    length = len(mystr)
                    if length > width:
                        width = length
        if width < 5:
            width = 5
        return width

    def getScanSpwSummary(mslist=[]):
        """Get a consolidated dictionary with scan, spw, channel information
           of a list of MSs. It adds the nrows of all sub-scans of a scan.

           Keyword arguments:
           mslist    --> list with names of MSs

           Returns a dictionary such as:
           mylist=['subms1.ms','subms2.ms']
           outdict = getScanSpwSummary(mylist)
           outdict = {0: {'MS': 'subms1.ms',
                          'scanId': {30: {'nchans': array([64, 64]),
                                          'nrows': 544,
                                          'spwIds': array([ 0,  1])}},
                          'size': '214M'},
                      1: {'MS': 'ngc5921.ms',
                          'scanId': {1: {'nchans': array([63]),
                                         'nrows': 4509,
                                         'spwIds': array([0])},
                                     2: {'nchans': array([63]),
                                         'nrows': 1890,
                                         'spwIds': array([0])}},
                          'size': '72M'}}
        """

        if mslist == []:
            return {}

        mslocal1 = ms()

        # Create lists for scan and spw dictionaries of each MS
        msscanlist = []
        msspwlist = []

        # List with sizes in bytes per sub-MS
        sizelist = []

        # Loop through all MSs
        for subms in mslist:
            try:
                mslocal1.open(subms)
                scans = mslocal1.getscansummary()
                msscanlist.append(scans)
                spws = mslocal1.getspectralwindowinfo()
                msspwlist.append(spws)
            except Exception as exc:
                raise Exception('Cannot get scan/spw information from subMS: {0}'.format(exc))
            finally:
                mslocal1.close()

            # Get the data volume in bytes per sub-MS
            sizelist.append(getDiskUsage(subms))

        # Get the information to list in output
        # Dictionary to return
        outdict = {}

        for ims in range(mslist.__len__()):
            # Create temp dictionary for each sub-MS
            tempdict = {}
            msname = os.path.basename(mslist[ims])
            tempdict['MS'] = msname
            tempdict['size'] = sizelist[ims]

            # Get scan dictionary for this sub-MS
            scandict = msscanlist[ims]

            # Get spw dictionary for this sub-MS
            # NOTE: the keys of spwdict.keys() are NOT the spw Ids
            spwdict = msspwlist[ims]

            # The keys are the scan numbers
            scanlist = scandict.keys()

            # Get information per scan
            tempdict['scanId'] = {}
            for scan in scanlist:
                newscandict = {}
                subscanlist = scandict[scan].keys()

                # Get spws and nrows per sub-scan
                nrows = 0
                aspws = np.array([],dtype='int32')
                for subscan in subscanlist:
                    nrows += scandict[scan][subscan]['nRow']

                    # Get the spws for each sub-scan
                    spwids = scandict[scan][subscan]['SpwIds']
                    aspws = np.append(aspws,spwids)

                newscandict['nrows'] = nrows

                # Sort spws  and remove duplicates
                aspws.sort()
                uniquespws = np.unique(aspws)
                newscandict['spwIds'] = uniquespws

                # Array to hold channels
                charray = np.empty_like(uniquespws)
                spwsize = np.size(uniquespws)

                # Now get the number of channels per spw
                for ind in range(spwsize):
                    spwid = uniquespws[ind]
                    for sid in spwdict.keys():
                        if spwdict[sid]['SpectralWindowId'] == spwid:
                            nchans = spwdict[sid]['NumChan']
                            charray[ind] = nchans
                            continue

                newscandict['nchans'] = charray
                tempdict['scanId'][int(scan)] = newscandict


            outdict[ims] = tempdict

        #pprint.pprint(outdict)

        return outdict

    mslocal = ms()
    mslocal1 = ms()
    ffout = None

    try:
        if (type(vis) == str) & os.path.exists(vis):
            mslocal.open(thems=vis)
        else:
            raise Exception('Visibility data set not found - please verify the name')

        # Check output filename existence
        if listfile is not None and isinstance(listfile,str) and listfile != '':
            if (type(listfile) == str) & os.path.exists(listfile):
                raise Exception('Output file \'%s\' already exists'%listfile)

            ffout = open(listfile, 'w')

        # Is it a multi-MS?
        ismms = mslocal.ismultims()

        # List of MSs to process
        mslist = []

        # It is a multi-MS
        if ismms:
            mslist = mslocal.getreferencedtables()
            mslist.sort()
            sname = 'Sub-MS'

            # Get the AxisType of the MMS
            axis = axisType(vis)
            if axis == '':
                axis = 'unknown'

        else:
            mslist.append(vis)
            sname = 'MS'

        # Close top MS
        mslocal.close()

        # Get a consolidated dictionary with scan, spw, channel information
        # of the list of subMSs. It adds the nrows of all sub-scans of a scan.
        try:
            outdict = {}
            outdict = getScanSpwSummary(mslist)
        except Exception as instance:
            print('%s ERROR'%instance)

        # Now loop through the dictionary to print the information
        indices = sorted(outdict.keys())

        counter = 0
        for index in indices:

            # Get data
            MS = outdict[index]['MS']
            SIZE = outdict[index]['size']
            SCAN = outdict[index]['scanId']

            # Sort scans for more optimal printing
            # Print information per scan
            firstscan = True
            skeys = sorted(SCAN.keys())
            for myscan in skeys:
                SPW = outdict[index]['scanId'][myscan]['spwIds']
                NCHAN = outdict[index]['scanId'][myscan]['nchans']
                NROWS = outdict[index]['scanId'][myscan]['nrows']

                # Get maximum widths
                smxw = getWidth(outdict, 'spw')
                cmxw = getWidth(outdict, 'channel')

                # Create format
                fhdr = '%-'+str(len(MS)+2)+'s' + '%-6s' + '%-'+str(smxw+2)+'s' + \
                        '%-'+str(cmxw+2)+'s' + '%-8s' + '%-6s'


                # Print header
                text = ''
                if counter == 0:
                    text = text + fhdr % (sname,'Scan','Spw','Nchan','Nrows','Size')
                    text = text + '\n'
                counter += 1

                # Print first scan
                if firstscan:
                    text = text + fhdr % (MS, myscan, SPW, NCHAN, NROWS, SIZE)
                else:
                    text = text + fhdr % ('', myscan, SPW, NCHAN, NROWS, '')

                firstscan = False

                # Print to a file
                if listfile is not None and isinstance(listfile,str) and listfile != '':
                    print >> ffout, text

        if listfile is not None and isinstance(listfile,str) and listfile != '':
            ffout.close()


        # Return the scan dictionary
        if createdict:
            return outdict

        return {}

    except Exception as instance:
#        mslocal.close()
        print('*** Error *** %s' % instance)


###########################
# beginning of actual test

class test_createmultims(unittest.TestCase):

    def checktable(self, thename, theexpectation):
        global msname, myname
        self.tb.open(msname+"/"+thename)
        for mycell in theexpectation:
            print("%s: comparing %s" % (myname,mycell))
            value = self.tb.getcell(mycell[0], mycell[1])
            # see if value is array
            try:
                isarray = value.__len__
            except:
                # it's not an array
                # zero tolerance?
                if mycell[3] == 0:
                    in_agreement = (value == mycell[2])
                else:
                    in_agreement = ( abs(value - mycell[2]) < mycell[3])
            else:
                # it's an array
                # zero tolerance?
                if mycell[3] == 0:
                    in_agreement =  (value == mycell[2]).all()
                else:
                    try:
                        in_agreement = (abs(value - mycell[2]) < mycell[3]).all()
                    except:
                        in_agreement = False
            if not in_agreement:
                print("%s:  Error in MS subtable %s:" % (myname,thename))
                print("     column %s row %s contains %s" % (mycell[0],mycell[1],value))
                print("     expected value is %s" % mycell[2])
                self.tb.close()
                return False
        self.tb.close()
        print("%s: table %s as expected." % (myname,thename))
        return True

    def setUp(self):
        res = None

        self.tb = table( )
        self.ms = ms( )
        datapath='unittest/mstool/'
        cpath = os.path.abspath(os.curdir)
        filespresent = sorted(glob.glob("part*.ms"))
        os.chdir(ctsys.resolve(datapath))
        for mymsname in sorted(glob.glob("part*.ms")):
            if not mymsname in filespresent:
                print("Copying %s" % mymsname)
                shutil.copytree(mymsname, cpath+'/'+mymsname)
        os.chdir(cpath)

    def tearDown(self):
        pass
        self.tb.done( )
        self.ms.done( )
        shutil.rmtree(msname,ignore_errors=True)
        shutil.rmtree("part2-mod.ms",ignore_errors=True)
        shutil.rmtree("part2-mod2.ms",ignore_errors=True)

    def test1(self):
        '''Test_createmultims 1: 4 parts, same sources but different spws'''
        retValue = {'success': True, 'msgs': "", 'error_msgs': '' }


        shutil.rmtree(msname,ignore_errors=True)

        self.res = self.ms.createmultims(msname,
                                    ['part1.ms','part2.ms','part3.ms','part4.ms'],
                                    [],
                                    True, # nomodify
                                    False,# lock
                                    True) # copysubtables from first to all other members
        self.ms.close()

        self.assertEqual(self.res,True)

        ldict = listpartition(vis=msname, createdict=True)

        self.assertEqual(sorted(ldict.keys()), [0, 1, 2, 3])

        self.assertEqual(ldict[0]['MS'].split('/').pop(), 'part1.ms')
        self.assertEqual(ldict[1]['MS'].split('/').pop(), 'part2.ms')
        self.assertEqual(ldict[2]['MS'].split('/').pop(), 'part3.ms')
        self.assertEqual(ldict[3]['MS'].split('/').pop(), 'part4.ms')

if __name__ == '__main__':
    unittest.main()
