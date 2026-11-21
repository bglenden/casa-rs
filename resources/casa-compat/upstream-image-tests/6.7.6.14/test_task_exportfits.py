#########################################################################
# test_task_exportfits.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.data.exportfits.html
#
##########################################################################
import os
import numpy
import sys
import shutil
import unittest

from casatools import image
from casatasks import exportfits

class exportfits_test(unittest.TestCase):

    def test_CAS3675(self):
        """ test fix for CAS 3675, outfile must be specified """
        name = "my.im"
        yy = image()
        yy.fromshape(name, [1,1,1,1])
        yy.done()
        self.assertRaises(Exception, exportfits, imagename=name, overwrite=True)

if __name__ == '__main__':
    unittest.main()
