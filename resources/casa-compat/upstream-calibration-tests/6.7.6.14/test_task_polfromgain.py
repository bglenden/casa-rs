##########################################################################
# test_task_polfromgain.py
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
# [Add the link to the JIRA ticket here once it exists]
#
# Based on the requirements listed in plone found here:
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.calibration.polfromgain.html
#
#
##########################################################################
import os
import unittest
import shutil
import time
from casatestutils import generate_weblog
from casatestutils import stats_dict
from casatestutils import add_to_dict
from casatestutils.compare import compare_caltables
from casatestutils.compare import compare_dictionaries

import casatools
from casatasks import polfromgain,gaincal
tb = casatools.table()
import sys
import os

import numpy as np


# DATA #
# these are not data appropriate for polfromgain and are no longer needed (gmoellen 2023Dec18):
#datapath = casatools.ctsys.resolve('unittest/polfromgain/ngc5921.ms/')
#caldata = casatools.ctsys.resolve('unittest/polfromgain/ngcgain.G0/')
#refcal = casatools.ctsys.resolve('unittest/polfromgain/polfromgainCalCompare.cal')

# The following should be mad a soft link within unittest/polfromgain:
datapath = casatools.ctsys.resolve('unittest/polfromgain/polcal_LINEAR_BASIS.ms/')

datacopy = 'polfromgaintest.ms'
caltab = 'polfromgaintest.G'
outcaltab = 'polfromgaintest.Gcor'

class polfromgain_test(unittest.TestCase):

    def setUp(self):
        shutil.copytree(datapath, datacopy)

    def tearDown(self):
        shutil.rmtree(datacopy)

        if os.path.exists(caltab):
            shutil.rmtree(caltab)

        if os.path.exists(outcaltab):
            shutil.rmtree(outcaltab)

    def test_pfg(self):

        # create a gain table with gaincal appropriate for deriving Q,U via polfromgain
        #  and manually flag some solutions within to facilitate test of min parang coverage)
        #  (this caltable could be generated and installed in the dr)
        gaincal(vis=datacopy,caltable=caltab,field='1',spw='1,2,3',solint='inf',refant='5',smodel=[1,0,0,0],minsnr=0.0)
        tb.open(caltab,nomodify=False)
        quer2='(ANTENNA1 IN [6,7,8,9] && SPECTRAL_WINDOW_ID==2 && TIME > 5009925000.0)'
        quer3='(ANTENNA1 IN [3,4,5,6,7,8,9] && SPECTRAL_WINDOW_ID==3 && TIME > 5009929000.0)'
        st=tb.query(quer2+' || '+quer3)
        fl=st.getcol('FLAG')
        fl[:,:,:]=True
        st.putcol('FLAG',fl)
        st.close()
        tb.close()

        # reference values
        ref1good=np.array([0.077, 0.061])
        ref2bad=np.array([0.073, 0.057])
        ref3bad=np.array([0.074, 0.066])

        # spws 2 and 3 are biased by poorer parang cov than spw 1
        Sa=polfromgain(vis=datacopy,tablein=caltab,caltable='',paoffset=0.0,minpacov=0.0)

        # Check output is a dict
        self.assertTrue(type(Sa) == type({}))

        # Check values for minpacov=0.0 (no constraint)
        Ra1=np.around(Sa['J2354-3600']['Spw1'][1:3],3)
        Ra2=np.around(Sa['J2354-3600']['Spw2'][1:3],3)
        Ra3=np.around(Sa['J2354-3600']['Spw3'][1:3],3)

        self.assertTrue(np.all(Ra1==ref1good))  # spw 1 is good
        self.assertTrue(np.all(Ra2==ref2bad))   # spw 2 is bad
        self.assertTrue(np.all(Ra3==ref3bad))   # spw 3 is bac

        # minpacov=30.0 excludes antennas with very poor parang cov in spw 2
        Sb=polfromgain(vis=datacopy,tablein=caltab,caltable='',paoffset=0.0,minpacov=30.0)
        Rb1=np.around(Sb['J2354-3600']['Spw1'][1:3],3)
        Rb2=np.around(Sb['J2354-3600']['Spw2'][1:3],3)
        Rb3=np.around(Sb['J2354-3600']['Spw3'][1:3],3)
        self.assertTrue(np.all(Rb1==ref1good))
        self.assertTrue(np.all(Rb2==ref1good))   # spw2 now good
        self.assertTrue(np.all(Rb3==ref3bad))    # spw3 still bad


        # minpacov=90.0 excludes antennas with very poor parang cov in spw 2 and 3
        Sc=polfromgain(vis=datacopy,tablein=caltab,caltable=outcaltab,paoffset=0.0,minpacov=90.0)
        Rc1=np.around(Sc['J2354-3600']['Spw1'][1:3],3)
        Rc2=np.around(Sc['J2354-3600']['Spw2'][1:3],3)
        Rc3=np.around(Sc['J2354-3600']['Spw3'][1:3],3)
        self.assertTrue(np.all(Rc1==ref1good))
        self.assertTrue(np.all(Rc2==ref1good))   # spw2 now good
        self.assertTrue(np.all(Rc3==ref1good))   # spw3 now good

        # check that an output caltable was generated
        self.assertTrue(os.path.exists(outcaltab))


        # minpacov=132.0 excludes all antennas and should return an empty dict for field 'J2354-3600'
        Sd=polfromgain(vis=datacopy,tablein=caltab,caltable='',paoffset=0.0,minpacov=132.0)
        self.assertTrue(Sd['J2354-3600']=={})


if __name__ == '__main__':
    unittest.main()
