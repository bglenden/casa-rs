#########################################################################
# test_task_appendantab.py
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
#
# Based on the requirements listed in casadocs found here:
#
#
##########################################################################
import os
import shutil
import unittest
from unittest.mock import patch

import numpy as np

from casatestutils import testhelper as th

from casatasks import appendantab
from casatools import ctsys, table

tb = table()

datapath = ctsys.resolve('unittest/appendantab')

# input data
evn_data = "idi1.ms"
evn_antab = os.path.join(datapath, "n14c3.antab")
append_data = "appendedData.ms"
evn_ref = os.path.join(datapath, "appendantab_test.ms")
data_copy = "dataCopy.ms"

vla_vis = os.path.join(datapath, 'tf042b1_try2.ms')
vla_copy = 'vla_copy.ms'
corrected_overwrite = 'corrected_overwrite.ms'
ref_overwrite = os.path.join(datapath, 'vla_appendantab_overwrite_ref.ms')
vla_antab = os.path.join(datapath, 'VLBA_7GHZ_GAINS.ANTAB')


def compareGains(table, ref):
    # Compare the gain values
    print("-----------------------------")
    gains = []
    tb.open(table+"/GAIN_CURVE")
    for i in range(tb.nrows()):
        gains.extend(tb.getcell('GAIN', i))
    time_new = tb.getcol('TIME')
    tb.close()

    ref_gains = []
    tb.open(ref+"/GAIN_CURVE")
    for i in range(tb.nrows()):
        cur = tb.getcell('GAIN', i)
        ref_gains.extend(tb.getcell('GAIN', i))
    time_orig = tb.getcol('TIME')
    interval_orig = tb.getcol('INTERVAL')
    tb.close()

    if len(ref_gains) != len(gains):
        return False

    for i in range(len(gains)):
        #print("COMP: ", ref_gains[i]- gains[i])
        #print("TRUTH: ", np.isclose(ref_gains[i], gains[i], atol=1e-6))
        if not np.all(np.isclose(ref_gains[i], gains[i])):
            print("REF AND REAL DIFFER: ", ref_gains[i], gains[i])
            return False

    return True


class appendantab_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        if os.path.exists(data_copy):
            shutil.rmtree(data_copy)
        shutil.copytree(os.path.join(datapath, evn_data), data_copy)

        if os.path.exists(vla_copy):
            shutil.rmtree(vla_copy)
        shutil.copytree(vla_vis, vla_copy)

    def tearDown(self):
        if os.path.exists(data_copy):
            shutil.rmtree(data_copy)

        if os.path.exists(vla_copy):
            shutil.rmtree(vla_copy)

        if os.path.exists(append_data):
            shutil.rmtree(append_data)

        if os.path.exists("secondAppend.ms"):
            shutil.rmtree("secondAppend.ms")

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(data_copy):
            shutil.rmtree(data_copy)

        if os.path.exists(append_data):
            shutil.rmtree(append_data)

        if os.path.exists(corrected_overwrite):
            shutil.rmtree(corrected_overwrite)

    def test_appendAll(self):
        """ Test appending both the GAIN_CURVE and SYSCAL tables """
        appendantab(vis=data_copy, outvis=append_data, antab=evn_antab, overwrite=False, append_tsys=True, append_gc=True)

        # compare to ref syscal and gain_curve table
        self.assertTrue(th.compTables(evn_ref+'/SYSCAL', append_data+'/SYSCAL', []))
        self.assertTrue(compareGains(append_data, evn_ref))

    def test_appendGainCurve(self):
        """ Test appending just the GAIN_CURVE table """
        appendantab(vis=data_copy, outvis=append_data, antab=evn_antab, overwrite=False, append_tsys=False, append_gc=True)

        # make sure SYSCAL wasn't created
        self.assertFalse(os.path.exists(append_data + "/SYSCAL"))
        self.assertTrue(compareGains(append_data, evn_ref))
        self.assertTrue(th.compTables(evn_ref+'/GAIN_CURVE', append_data+'/GAIN_CURVE', ['GAIN']))

    def test_appendSysCal(self):
        """ Test appending just the SYSCAL table """
        appendantab(vis=data_copy, outvis=append_data, antab=evn_antab, overwrite=False, append_tsys=True, append_gc=False)

        # make sure GAIN_CURVE wasn't created
        self.assertFalse(os.path.exists(append_data + "/GAIN_CURVE"))

        # compare to ref SYSCAL table
        self.assertTrue(th.compTables(evn_ref+'/SYSCAL', append_data+'/SYSCAL', []))

    def test_appendToExistingSyscal(self):
        """ Test that the task appends data to an existing SYSCAL table """
        appendantab(vis=data_copy, outvis=append_data, antab=evn_antab, overwrite=False, append_tsys=True, append_gc=False)
        # Get the number of rows added
        tb.open(append_data + "/SYSCAL", nomodify=False)
        first_run = tb.nrows()

        # Change the syscal table so some antenna times dont match
        time = tb.getcol('TIME')
        time[-10:] = -1
        tb.putcol('TIME', time)
        tb.close()

        appendantab(vis=append_data, outvis="secondAppend.ms", antab=evn_antab, overwrite=False, append_tsys=True, append_gc=False)
        # Get the number of rows after consecutive runs
        tb.open("secondAppend.ms/SYSCAL")
        next_run = tb.nrows()
        tb.close()

        self.assertTrue(next_run > first_run)

    def test_appendToExistingGainCurve(self):
        """ Test that the task appends data to an existing GAIN_CURVE table """
        appendantab(vis=data_copy, outvis=append_data, antab=evn_antab, overwrite=False, append_tsys=False, append_gc=True)
        # Get the number of rows added
        tb.open(append_data + "/GAIN_CURVE", nomodify=False)
        first_run = tb.nrows()

        # Change the syscal table so some antenna times dont match
        time = tb.getcol('TIME')
        time[-10:] = -1
        tb.putcol('TIME', time)
        tb.close()

        appendantab(vis=append_data, outvis="secondAppend.ms", antab=evn_antab, overwrite=False, append_tsys=False, append_gc=True)
        # Get the number of rows after consecutive runs
        tb.open("secondAppend.ms/GAIN_CURVE")
        next_run = tb.nrows()
        tb.close()

        self.assertTrue(next_run > first_run)



    def test_Overwrite(self):

        # Test using the data on ticket
        appendantab(vis=vla_copy, outvis=corrected_overwrite,
                     antab=vla_antab, overwrite=True,
                     append_tsys=False, append_gc=True)

        # Check that the antenna values are correct across all spws
        tb.open(ref_overwrite+ '/GAIN_CURVE')
        sens_ref = tb.getcol('SENSITIVITY')
        gain_ref = tb.getcol('GAIN')
        tb.close()

        tb.open(corrected_overwrite+ '/GAIN_CURVE')
        sens = tb.getcol('SENSITIVITY')
        gain = tb.getcol('GAIN')
        tb.close()

        self.assertTrue(np.all(sens == sens_ref))
        self.assertTrue(np.all(gain == gain_ref))




if __name__ == '__main__':
    unittest.main()
