##########################################################################
# test_tool_coordsys.py
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
# https://casadocs.readthedocs.io/en/latest/api/tt/casatools.coordsys.html
#
#
##########################################################################
import shutil
import unittest
import os

from casatools import image as iatool
from casatools import coordsys
from casatools import ctsys
from casatools import table
from casatools import measures
from casatools import quanta
import numpy as np
import re
import math

me = measures()
qa = quanta()
cs = coordsys()
ia = iatool()

myim = "center_0.fits"

datapath='unittest/coordsys/'

def freqVelSetUp():
    mycs = cs.newcoordsys(spectral=True)

    # Set rest freq to reference freq
    rv = mycs.referencevalue(format='n')
    restFreq = rv['numeric'][0]
    mycs.setrestfrequency(restFreq)

    # Find radio velocity increment
    df = mycs.increment(format='n')
    c = qa.constants('c')['value']  # m/s
    drv = -c * df['numeric'] / rv['numeric'][0] / 1000.0  # km/s
    #
    freq = rv['numeric'][0]
    freqUnit = mycs.units()

    return mycs, rv, restFreq, df, c, drv, freq, freqUnit

def alleq(x,y,tolerance=0):
    if x.size != y.size:
        return False
    if len(x.shape)==1:
        for i in range(len(x)):
            if abs(x[i] - y[i]) >= tolerance:
                return False
    if len(x.shape)==2:
        for i in range(len(x)):
            for j in range(len(x[i])):
                if abs(x[i][j] - y[i][j]) >= tolerance:
                    return False
    if len(x.shape)==3:
        for i in range(len(x)):
            for j in range(len(x[i])):
                for k in range(len(x[i][j])):
                    if abs(x[i][j][k] - y[i][j][k]) >= tolerance:
                        return False
    if len(x.shape)==4:
        for i in range(len(x)):
            for j in range(len(x[i])):
                for k in range(len(x[i][j])):
                    for l in range(len(x[i][j][k])):
                        if abs(x[i][j][k][l] - y[i][j][k][l]) >= tolerance:
                             return False
    if len(x.shape)>4:
        print('unhandled array shape in alleq')
    return True

class coordsys_test(unittest.TestCase):

    def setUp(self):
        shutil.copy(ctsys.resolve(datapath + myim), myim)

    def tearDown(self):
        cs.done()
        os.remove(myim)
        tb = table( )
        self.assertTrue(len(tb.showcache()) == 0)
        tb.done( )

    def test_CAS_2724(self):
        myia = iatool()
        myia.open(myim)
        mycsys = myia.coordsys()
        self.assertRaises(Exception, mycsys.toworldmany, [1])

    def test_constructor(self):
        """Test constructors"""
        x = coordsys( )

    def test_findaxisbyname(self):
        myia = iatool()
        myia.fromshape("", [4, 4, 4, 4])
        csys = myia.coordsys( )
        myia.done()

        self.assertTrue(csys.findaxisbyname("ri") == 0)
        self.assertRaises(Exception, csys.findaxisbyname, "ra", False)
        self.assertTrue(csys.findaxisbyname("ra", True) == 0)

    def test_transpose(self):
        """Test the transpose() method"""
        myia = iatool()
        myia.fromshape("", [4, 4, 4, 4])
        csys = myia.coordsys()
        myia.done()

        orig = csys.names()
        self.assertTrue(csys.transpose([3,2,1,0]))
        new = csys.names()
        self.assertTrue(orig[0] == new[3])
        self.assertTrue(orig[1] == new[2])
        self.assertTrue(orig[2] == new[1])
        self.assertTrue(orig[3] == new[0])

    def test_findaxis(self):
        """Test the findaxis() method"""
        myia = iatool()
        myia.fromshape("", [20, 20, 4, 20])
        csys = myia.coordsys()
        self.assertRaises(Exception, csys.findaxis, True, -1)
        self.assertRaises(Exception, csys.findaxis, True, 4)
        got = csys.findaxis(True, 0)
        self.assertTrue(got == {'axisincoordinate': 0, 'coordinate': 0})
        got = csys.findaxis(True, 1)
        self.assertTrue(got == {'axisincoordinate': 1, 'coordinate': 0})
        got = csys.findaxis(True, 2)
        self.assertTrue(got == {'axisincoordinate': 0, 'coordinate': 1})
        got = csys.findaxis(True, 3)
        self.assertTrue(got == {'axisincoordinate': 0, 'coordinate': 2})
        myia.done()

    def test_findcoordinate(self):
        """Test the findcoordinate() method"""
        myia = iatool()
        myia.fromshape("", [20, 20, 4, 20])
        csys = myia.coordsys()
        myia.done()
        self.assertFalse(csys.findcoordinate('linear')['return'])
        self.assertFalse(
            csys.findcoordinate('direction', 2)['return']
        )
        got = csys.findcoordinate("direction")
        self.assertTrue(
            got['return'] and (got['pixel'] == [0,1]).all()
            and (got['world'] == [0,1]).all()
        )
        got = csys.findcoordinate("spectral")
        self.assertTrue(
            got['return'] and (got['pixel'] == [3]).all()
            and (got['world'] == [3]).all()
        )
        got = csys.findcoordinate("stokes")
        self.assertTrue(
            got['return'] and (got['pixel'] == [2]).all()
            and (got['world'] == [2]).all()
        )

    # ===== Test cases from casa5 coordsys regression test =====

    def test_constructedSetUp(self):
        """Test that the coordsys was set up with the right number of coords and type"""

        csys = coordsys()
        mycs = csys.newcoordsys()

        self.assertTrue(mycs.ncoordinates() == 0)
        self.assertTrue(mycs.type() == 'coordsys')
        mycs.done()

    def test_createDirectionAxes(self):
        """Test that a coordsys with direction axes can be created properly"""

        csys = coordsys()
        mycs = csys.newcoordsys(direction = True)
        t1 = mycs.axiscoordinatetypes(True)
        t2 = mycs.axiscoordinatetypes(False)

        self.assertTrue(mycs.ncoordinates() == 1)
        self.assertTrue(mycs.coordinatetype(0) == ['Direction'])
        self.assertTrue(t1[1] == 'Direction' and t1[1] == 'Direction')
        self.assertTrue(t2[0] == 'Direction' and t2[1] == 'Direction')
        mycs.done()


    def test_createSpectralAxes(self):
        """Test that coordsys with spectral axes can be created properly"""

        csys = coordsys()
        mycs = csys.newcoordsys(spectral=True)
        t1 = mycs.axiscoordinatetypes(True)
        t2 = mycs.axiscoordinatetypes(False)

        self.assertTrue(mycs.ncoordinates() == 1)
        self.assertTrue(mycs.coordinatetype(0) == ['Spectral'])
        self.assertTrue(t1 == ['Spectral'] and t2 == ['Spectral'])
        mycs.done()

    def test_createStokesAxes(self):
        """Test that coordsys with stokes axes can be created properly"""

        csys = coordsys()
        mycs = csys.newcoordsys(stokes="I Q U V")
        t1 = mycs.axiscoordinatetypes(True)
        t2 = mycs.axiscoordinatetypes(False)

        self.assertTrue(mycs.ncoordinates() == 1)
        self.assertTrue(mycs.coordinatetype(0) == ['Stokes'])
        self.assertTrue(t1 == ['Stokes'] and t2 == ['Stokes'])
        mycs.done()

    def test_createLinearAxes(self):
        """"Test that coordsys with linear axes can be created properly"""

        csys = coordsys()
        mycs = csys.newcoordsys(linear=3)
        t1 = mycs.axiscoordinatetypes(True)
        t2 = mycs.axiscoordinatetypes(False)

        self.assertTrue(mycs.ncoordinates() == 1)
        self.assertTrue(mycs.coordinatetype(0) == ['Linear'])
        self.assertTrue(t1[0] == 'Linear' and t2[0] == 'Linear')
        mycs.done()

    def test_createTabularAxes(self):
        """Test that coordsys with tabular axes can be created properly"""

        csys = coordsys()
        mycs = csys.newcoordsys(tabular=True)
        t1 = mycs.axiscoordinatetypes(True)
        t2 = mycs.axiscoordinatetypes(False)

        self.assertTrue(mycs.ncoordinates() == 1)
        self.assertTrue(mycs.coordinatetype(0) == ['Tabular'])
        self.assertTrue(t1 == ['Tabular'] and t2 == ['Tabular'])
        mycs.done()

    def test_createMixedAxes(self):
        """Test that a coordsys with mixed axes types can be created properly"""

        csys = coordsys()
        mycs = csys.newcoordsys(direction=True, spectral=True, stokes="I Q U V",
                              linear=1, tabular=True)
        types = mycs.coordinatetype()
        typesRes = ['Direction', 'Stokes', 'Spectral', 'Linear', 'Tabular']

        self.assertTrue(mycs.ncoordinates() == 5)
        self.assertTrue(np.all(types == typesRes))

        t1 = mycs.axiscoordinatetypes(True)
        t2 = mycs.axiscoordinatetypes(False)

        tRes = ['Direction', 'Direction', 'Stokes', 'Spectral', 'Linear', 'Tabular']

        self.assertTrue(np.all(t1 == tRes))
        self.assertTrue(np.all(t2 == tRes))
        mycs.done()

    def test_refCode(self):
        """Test that a reference code can be set"""

        csys = coordsys()
        mycs = csys.newcoordsys(direction=True, spectral=True)
        d = me.frequency("LSRK")
        list = me.listcodes(d)

        for i in list['normal']:
            if (i!='REST'):
                mycs.setreferencecode(type='spectral', value=i, adjust=True)
                if (mycs.referencecode(type='spectral')!=[i]):
                    self.fail()
        self.assertTrue(True)

    def test_setDirection(self):
        """Test that direction can be set"""

        csys = coordsys()
        mycs = csys.newcoordsys(direction=True, spectral=True)
        d = me.direction('J2000')
        list = me.listcodes(d)
        for i in list['normal']:
            if not re.search('AZEL', i, re.IGNORECASE):
                mycs.setreferencecode(type='direction', value=i, adjust=False)
                if (mycs.referencecode(type='direction')!=[i]):
                    self.fail()
        mycs = csys.newcoordsys(direction=True, spectral=True, linear=1)
        mycs.setreferencecode(type='direction', value='B1950')
        mycs.setreferencecode(type='spectral', value='BARY')
        c = mycs.referencecode()

        self.assertTrue(len(c) == 3)
        self.assertTrue(c[0] == 'B1950')
        self.assertTrue(c[1] == 'BARY')
        self.assertTrue(c[2] == '')

    def test_projection(self):
        """Test that the projection can be set"""

        csys = coordsys()
        mycs = csys.newcoordsys(direction=True, spectral=True, linear=1)
        mycs.setprojection('SIN', [1.0, 2.0])
        p = mycs.projection()

        self.assertTrue(p['type'] == 'SIN' and len(p['parameters']) == 2)
        self.assertTrue(p['parameters'][0] == 1.0)
        self.assertTrue(p['parameters'][1] == 2.0)

        p = mycs.projection('all')['types']
        self.assertTrue(len(p) == 27)
        for i in p:
            n = mycs.projection(i)
            if not n: self.fail()

    def test_restFreq(self):
        """Test that the rest frequency can be set"""

        csys = coordsys()
        mycs = csys.newcoordsys(direction=True, spectral=True)
        rf1 = qa.quantity('1.2GHz')
        mycs.setrestfrequency(rf1)
        rf2 = mycs.restfrequency()
        rf2 = qa.convert(rf2, rf1['unit'])

        self.assertTrue(abs(qa.getvalue(rf1) - qa.getvalue(rf2)) < 1.0e-6)
        self.assertTrue(qa.getunit(rf1) == qa.getunit(rf2))

        unit = qa.getunit(mycs.restfrequency())
        rf1 = 2.0
        mycs.setrestfrequency(rf1)
        rf2 = mycs.restfrequency()
        rf1 = qa.unit(rf1, unit)

        self.assertTrue(abs(qa.getvalue(rf1)-qa.getvalue(rf2))<1.0e-6)
        self.assertTrue(qa.getunit(rf1)==qa.getunit(rf2))

        rf1 = qa.quantity([1e9, 2e9], 'Hz')
        # Select second freq
        mycs.setrestfrequency(value=rf1, which=1, append=False)
        rf2 = qa.convert(mycs.restfrequency(), qa.getunit(rf1))
        v1 = qa.getvalue(rf1)
        v2 = qa.getvalue(rf2)

        self.assertTrue(abs(v1[0] - v2[1]) < 1e-6)
        self.assertTrue(abs(v1[1] - v2[0]) < 1e-6)
        self.assertTrue(qa.getunit(rf1) == qa.getunit(rf2))

    def test_toRecord(self):
        """Test that the coordsys can be converted to a record"""

        csys = coordsys()
        mycs = csys.newcoordsys(direction=True, spectral=True, stokes="I Q U V", linear=3)

        rec = mycs.torecord()

        self.assertTrue('direction0' in rec)
        self.assertTrue('stokes1' in rec)
        self.assertTrue('spectral2' in rec)
        self.assertTrue('linear3' in rec)

    def test_fromRecord(self):
        """Test that a record can be converted to a coordsys"""

        csys = coordsys()
        mycs = csys.newcoordsys(direction=True, spectral=True, stokes="I Q U V", linear=3)

        rec = mycs.torecord()
        cs2 = csys.newcoordsys(direction=False, spectral=False, stokes="", linear=0)
        self.assertTrue(cs2.fromrecord(rec))
        mycs.done()
        cs2.done()

    def test_copy(self):
        """Test that the coordsys can be copied"""

        csys = coordsys()
        mycs = csys.newcoordsys(direction=True)
        cs2 = mycs.copy()

        self.assertTrue(cs2)
        closed = mycs.done()
        self.assertTrue(closed and cs2)
        cs2.done()

    def test_epoch(self):
        """Test that you can set and get the epoch"""

        csys = coordsys()
        mycs = csys.newcoordsys(direction=True)
        epoch1 = me.epoch('UTC', 'today')
        mycs.setepoch(epoch1)
        epoch2 = mycs.epoch()

        self.assertTrue(abs(me.getvalue(epoch1)['m0']['value']-me.getvalue(epoch2)['m0']['value'])<1.0e-6)
        self.assertTrue(me.getvalue(epoch1)['m0']['unit'] == me.getvalue(epoch2)['m0']['unit'])
        self.assertTrue(me.gettype(epoch1) == me.gettype(epoch2))
        self.assertTrue(me.getref(epoch1) == me.getref(epoch2))
        mycs.done()

    def test_observer(self):
        """Test that you can set and get the observer"""
        csys = coordsys()
        mycs = csys.newcoordsys(direction=True)
        obs1 = 'Biggles'
        mycs.setobserver(obs1)
        obs2 = mycs.observer()

        self.assertTrue(obs1 == obs2)
        mycs.done()

    def test_telescope(self):
        """Test that you can set and get the telescope"""
        csys = coordsys()
        mycs = csys.newcoordsys(direction=True)
        tel1 = 'VLA'
        mycs.settelescope(tel1)
        tel2 = mycs.telescope()

        self.assertTrue(tel1 == tel2)
        mycs.done()

    def test_parentName(self):
        """Test that you can set and get the parent name"""
        csys = coordsys()
        mycs = csys.newcoordsys(direction=True)
        pn1 = 'Biggles.image'
        mycs.setparentname(pn1)
        pn2 = mycs.parentname()

        self.assertTrue(pn1 == pn2)
        mycs.done()

    def test_names(self):
        """Test that you can set and get names"""

        mycs = cs.newcoordsys(direction=True, spectral=True)
        val1 = "a b c"
        mycs.setnames(value=val1)
        val2 = mycs.names()
        self.assertTrue(val1.split() == val2)

        val2 = mycs.names('spec')
        self.assertTrue(val2[0] == val1.split()[2])

        val1 = 'fish'
        mycs.setnames(type='spec', value=val1)
        val2 = mycs.names('spec')
        self.assertTrue(val2==[val1])
        mycs.done()

    def test_units(self):
        """Test that you can set and get unit names"""

        mycs = cs.newcoordsys(direction=True, spectral=True)
        val1 = "deg rad GHz"
        mycs.setunits(value=val1)
        val2 = mycs.units()

        self.assertTrue(np.all(val1.split() == val2))
        # self.assertFalse(mycs.setunits(value="Hz Hz Hz"))
        # self.assertFalse(mycs.setunits(value="m"))

        val1 = 'deg rad GHz'
        mycs.setunits(value=val1)
        val2 = mycs.units('spec')
        self.assertTrue(val2[0] == val1.split()[2])

        val1 = "kHz"
        mycs.setunits(type='spec', value=val1)
        val2 = mycs.units('spec')
        self.assertTrue(val2[0] == val1)

        mycs = cs.newcoordsys(direction=True, linear=2)
        val1 = "Hz kHz"
        mycs.setunits(type='linear', value=val1, overwrite=True)
        val2 = mycs.units()

        self.assertTrue(val1.split()[0] == val2[2] and val1.split()[1] == val2[3])
        mycs.done()

    def test_refPixel(self):
        """Test that you can get and set the reference pixel"""

        mycs = cs.newcoordsys(direction=True, spectral=True)
        val1 = [0, 1, 2]
        mycs.setreferencepixel(value=val1)
        val2 = mycs.referencepixel()['numeric']
        self.assertTrue(abs(val1[0]-val2[0]) < 1.0e-6)
        self.assertTrue(abs(val1[1]-val2[1]) < 1.0e-6)
        self.assertTrue(abs(val1[2]-val2[2]) < 1.0e-6)

        val2 = mycs.referencepixel('dir')['numeric']
        self.assertTrue(abs(val2[0] - val1[0]) < 1.0e-6)
        self.assertTrue(abs(val2[1] - val1[1]) < 1.0e-6)

        val2 = mycs.referencepixel('spec')['numeric']
        self.assertTrue(abs(val2 - val1[2]) < 1.0e-6)

        val1 = [0, 0]
        mycs.setreferencepixel(type='dir', value=val1)
        val2 = mycs.referencepixel('dir')['numeric']
        self.assertTrue(abs(val2[0] - val1[0]) < 1e-6)
        self.assertTrue(abs(val2[1] - val1[1]) < 1e-6)

        mycs.done()

    def test_linearTransformDirection(self):
        """Test set linear transform type to direction"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes='IQ', tabular=True, linear=3)
        val1 = ia.makearray(0, [2, 2])
        val1[0, 0] = math.cos(0.5)
        val1[0, 1] = -math.sin(0.5)
        val1[1, 0] = math.sin(0.5)
        val1[1, 1] = math.cos(0.5)
        type = 'direction'
        mycs.setlineartransform(value=val1, type=type)
        val2 = mycs.lineartransform(type=type)

        self.assertTrue(np.all(np.isclose(val1, val2, atol=1.0e-6)))

    def test_linearTransformSpectral(self):
        """Test set linear transform type to spectral """
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes='IQ', tabular=True, linear=3)
        val1 = ia.makearray(2, [1, 1])
        type = 'spectral'
        mycs.setlineartransform(value=val1, type=type)
        val2 = mycs.lineartransform(type=type)
        self.assertTrue(np.all(np.isclose(val1, val2, atol=1.0e-6)))

    def test_linearTransformStokes(self):
        """Test set linear transform type to stokes"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes='IQ', tabular=True, linear=3)
        val1 = ia.makearray(2, [1, 1])
        type = 'stokes'
        mycs.setlineartransform(value=val1, type=type)
        val2 = mycs.lineartransform(type=type)
        self.assertFalse(np.all(np.isclose(val1, val2, atol=1.0e-6)))

    def test_linearTransformTabular(self):
        """Test set linear transform type to tabular"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes='IQ', tabular=True, linear=3)
        val1 = ia.makearray(4, [1, 1])
        type = 'tabular'
        mycs.setlineartransform(value=val1, type=type)
        val2 = mycs.lineartransform(type=type)
        self.assertTrue(np.all(np.isclose(val1, val2, atol=1.0e-6)))

    def test_linearTransformLinear(self):
        """Test set linear transform type to linear"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes='IQ', tabular=True, linear=3)
        val1 = ia.makearray(0, [3, 3])
        val1[0, 0] = 2.0
        val1[1, 1] = 3.0
        val1[2, 2] = 4.0
        type = 'linear'
        mycs.setlineartransform(value=val1, type=type)
        val2 = mycs.lineartransform(type=type)
        self.assertTrue(np.all(np.isclose(val1, val2, atol=1.0e-6)))

    def test_referenceValueSetValRad(self):
        """Test set reference value in radians"""
        mycs = cs.newcoordsys(direction=True)
        mycs.setunits(value="rad rad")
        val1 = mycs.referencevalue(format='q')
        val1['quantity']['*1'] = qa.quantity('0.01rad')
        val1['quantity']['*2'] = qa.quantity('-0.01rad')
        mycs.setreferencevalue(value=val1)
        val2 = mycs.referencevalue(format='q')

        self.assertTrue(abs(val1['quantity']['*1']['value'] - val2['quantity']['*1']['value']) < 1e-6)
        self.assertTrue(abs(val1['quantity']['*2']['value'] - val2['quantity']['*2']['value']) < 1e-6)
        self.assertTrue(val1['quantity']['*1']['unit'] == val2['quantity']['*1']['unit'])
        self.assertTrue(val1['quantity']['*2']['unit'] == val2['quantity']['*2']['unit'])
        mycs.done()

    def test_referenceValueSetValDefaultUnits(self):
        """Test set reference value using default units"""
        mycs = cs.newcoordsys(direction=True)
        units = mycs.units()
        val1 = [1.0, 2.0]
        mycs.setreferencevalue(value=val1)
        val2 = mycs.referencevalue(format='q')
        self.assertTrue(abs(val1[0] - val2['quantity']['*1']['value']) < 1e-6)
        self.assertTrue(abs(val1[1] - val2['quantity']['*2']['value']) < 1e-6)
        self.assertTrue(units[0] == val2['quantity']['*1']['unit'])
        self.assertTrue(units[1] == val2['quantity']['*2']['unit'])
        mycs.done()

    def test_referenceValueSetIncorrect(self):
        """Test setting the value incorrectly"""
        mycs = cs.newcoordsys(spectral=True)
        try:
            ok = True
            mycs.setreferencevalue(value='i like doggies')
        except Exception as e:
            ok = False
        self.assertFalse(ok)
        mycs.done()

    def test_referenceValueSetSpectral(self):
        """Test set reference value with spectral type"""
        mycs = cs.newcoordsys(direction=True, spectral=True)
        val1 = mycs.referencevalue(format='q')
        val2 = mycs.referencevalue(type='spec', format='q')
        self.assertTrue(abs(val1['quantity']['*3']['value'] - val2['quantity']['*1']['value']) < 1e-6 and val1['quantity']['*3'][
            'unit'] == val2['quantity']['*1']['unit'])

        val1 = [-10]
        mycs.setreferencevalue(type='spec', value=val1)
        val2 = mycs.referencevalue(type='spec', format='n')
        self.assertTrue(abs(val1[0] - val2['numeric'][0]) < 1e-6)

        val1 = mycs.referencevalue(format='n')
        val2 = mycs.referencevalue(type='spec', format='n')
        self.assertTrue(abs(val1['numeric'][2] - val2['numeric'][0]) < 1e-6)
        mycs.done()

    def test_incrementRad(self):
        """Test increment with radian units"""
        mycs = cs.newcoordsys(direction=True)
        mycs.setunits(value="rad rad")
        val1 = mycs.increment(format='q')
        val1['quantity']['*1'] = qa.quantity('0.01rad')
        val1['quantity']['*2'] = qa.quantity('-0.01rad')
        mycs.setincrement(value=val1)
        val2 = mycs.increment(format='q')
        #
        self.assertTrue(abs(val1['quantity']['*1']['value'] - val2['quantity']['*1']['value']) < 1e-6)
        self.assertTrue(abs(val1['quantity']['*2']['value'] - val2['quantity']['*2']['value']) < 1e-6)
        self.assertTrue(val1['quantity']['*1']['unit'] == val2['quantity']['*1']['unit'])
        self.assertTrue(val1['quantity']['*2']['unit'] == val2['quantity']['*2']['unit'])
        mycs.done()

    def test_incrementDefaultUnits(self):
        """Test increment with default coordsys units"""
        mycs = cs.newcoordsys(direction=True)
        units = mycs.units()
        val1 = [1.0, 2.0]
        mycs.setincrement(value=val1)
        val2 = mycs.increment(format='q')

        self.assertTrue(abs(val1[0] - val2['quantity']['*1']['value']) < 1e-6)
        self.assertTrue(abs(val1[1] - val2['quantity']['*2']['value']) < 1e-6)
        self.assertTrue(units[0] == val2['quantity']['*1']['unit'])
        self.assertTrue(units[1] == val2['quantity']['*2']['unit'])
        mycs.done()
        #

    def test_incrementSetIncorrect(self):
        """Test increment with incorrect values"""
        mycs = cs.newcoordsys(spectral=True)
        try:
            ok = True
            mycs.setincrement(value='i like doggies')
        except Exception as e:
            ok = False
        mycs.done()
        self.assertFalse(ok)

    def test_incrementSpectral(self):
        """Test increment using spectral type"""
        mycs = cs.newcoordsys(direction=True, spectral=True)
        val1 = [1.0, 2.0, 3.0]
        mycs.setincrement(value=val1)
        val2 = mycs.increment(format='n', type='dir')
        self.assertTrue(abs(val2['numeric'][0] - val1[0]) < 1e-6 and abs(val2['numeric'][1] - val1[1]) < 1e-6)
        val2 = mycs.increment(type='spe', format='n')
        self.assertTrue(abs(val2['numeric'][0] - val1[2]) < 1e-6)
        try:
            val2 = mycs.increment(type='lin', format='q')
        except Exception as e:
            val2 = False
            self.assertTrue(True)
        if val2: self.fail()
        #
        val1 = [-10]
        mycs.setincrement(type='spec', value=val1)
        val2 = mycs.increment(type='spec', format='n')
        self.assertTrue(abs(val1[0] - val2['numeric'][0]) < 1e-6)
        mycs.done()

    def test_stokes(self):
        """Test that you can create a coordsys with the given stokes values"""
        mycs = cs.newcoordsys(stokes="I RL")
        stokes = mycs.stokes()

        self.assertTrue(stokes[0] == 'I' and stokes[1] == 'RL', msg='Stokes recovered wrong values')
        mycs.done()

    def test_stokesSetValue(self):
        """Test that you can set the stokes values after creation"""
        mycs = cs.newcoordsys(stokes="I RL")
        mycs.setstokes("XX V")
        stokes = mycs.stokes()

        self.assertTrue(stokes[0] == 'XX' and stokes[1] == 'V', msg='Stokes recovered wrong values')
        mycs.done()

    def test_stokesWrongValues(self):
        """Test that incorrect values raise exceptions"""
        mycs = cs.newcoordsys(direction=True)
        try:
            stokes = True
            stokes = mycs.stokes()
        except Exception as e:
            stokes = False
        self.assertFalse(stokes)

        try:
            ok = True
            ok = mycs.setstokes("I V")
        except Exception as e:
            ok = False
        self.assertFalse(ok)
        mycs.done()

    def test_toWorld(self):
        """Test the toworld conversion"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes="I V", linear=2)
        rp = mycs.referencepixel()['numeric']
        rv = mycs.referencevalue(format='n')

        self.assertTrue(len(rp) == 6)
        self.assertTrue(np.all(np.isclose(mycs.toworld(value=rp, format='n')['numeric'], rv['numeric'], atol=1e-6)))
        ###
        d = mycs.toworld(value=list(rp), format='q')
        u = mycs.units()

        self.assertTrue(len(d['quantity']) == len(rv['numeric']))

        for i in range(len(d['quantity'])):
            self.assertFalse(abs(d['quantity']['*' + str(i + 1)]['value'] - rv['numeric'][i]) > 1e-6)
            self.assertTrue(d['quantity']['*' + str(i + 1)]['unit'] == u[i])
        ###
        q = mycs.toworld(value=rp, format='q')
        m = mycs.toworld(value=rp, format='m')
        m = m['measure']

        self.assertTrue('direction' in m and 'spectral' in m)
        self.assertTrue('frequency' in m['spectral'])
        self.assertTrue('opticalvelocity' in m['spectral'])
        self.assertTrue('radiovelocity' in m['spectral'])
        self.assertTrue('betavelocity' in m['spectral'])
        self.assertTrue('stokes' in m)
        self.assertTrue('linear' in m)

        d = m['direction']
        f = m['spectral']['frequency']
        l = m['linear']
        s = m['stokes']

        v = me.getvalue(d)
        q['quantity']['*1'] = qa.convert(q['quantity']['*1'], v['m0']['unit'])
        q['quantity']['*2'] = qa.convert(q['quantity']['*2'], v['m1']['unit'])

        self.assertTrue(abs(v['m0']['value'] - q['quantity']['*1']['value']) < 1e-6)
        self.assertTrue(abs(v['m1']['value'] - q['quantity']['*2']['value']) < 1e-6)
        self.assertTrue(v['m0']['unit'] == q['quantity']['*1']['unit'])
        self.assertTrue(v['m1']['unit'] == q['quantity']['*2']['unit'])

        v = me.getvalue(f)
        q['quantity']['*4'] = qa.convert(q['quantity']['*4'], v['m0']['unit'])

        self.assertTrue(abs(v['m0']['value'] - q['quantity']['*4']['value']) < 1e-6)
        self.assertTrue(v['m0']['unit'] == q['quantity']['*4']['unit'])

        q['quantity']['*5'] = qa.convert(q['quantity']['*5'], l['*1']['unit'])
        q['quantity']['*6'] = qa.convert(q['quantity']['*6'], l['*2']['unit'])

        self.assertTrue(abs(l['*1']['value'] - q['quantity']['*5']['value']) < 1e-6)
        self.assertTrue(abs(l['*2']['value'] - q['quantity']['*6']['value']) < 1e-6)
        self.assertTrue(l['*1']['unit'] == q['quantity']['*5']['unit'])
        self.assertTrue(l['*2']['unit'] == q['quantity']['*6']['unit'])
        mycs.done()


    def test_toWorldMany(self):
        """Test the toworldmany parameter"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes="I V", linear=2)

        p = mycs.referencepixel()['numeric']
        w = mycs.referencevalue()
        rIn = ia.makearray(0, [len(p), 10])
        for i in range(10):
            for j in range(len(p)):
                rIn[j, i] = p[j]
        rOut = mycs.toworldmany(rIn)

        self.assertTrue(len(rOut['numeric']) == len(rIn))
        for i in range(10):
            for j in range(len(p)):
                self.assertTrue((rOut['numeric'][j, i] - w['numeric'][j]) < 1e-6)
        mycs.done()

    def test_toPixel(self):
        """Test topixel parameter"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes="I V", linear=2)
        tol = 1.0e-6
        rp = mycs.referencepixel()['numeric']

        self.assertTrue(len(rp) == 6)

        rv = mycs.referencevalue(format='n')
        p = mycs.topixel(value=rv)['numeric']

        self.assertTrue(len(p) == 6)
        self.assertTrue(np.all(np.isclose(p, rp, atol=tol)))

        for format in ["n", "q", "m", "s", "nqms"]:
            for i in range(len(rp)): p[i] = rp[i] + 1
            w = mycs.toworld(value=p, format=format)
            p2 = mycs.topixel(value=w)['numeric']

            self.assertTrue(len(p2) == 6)
            self.assertTrue(np.all(np.isclose(p, p2, atol=tol)),
                            msg='toworld/topixel reflection failed for format "' + format + '"')
        mycs.done()

    def test_toPixelMany(self):
        """Test topixelmany parameter"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes="I V", linear=2)
        n = 10
        p = mycs.referencepixel()['numeric']
        w = mycs.toworld(p, 'n')
        w = w['numeric']
        rIn = ia.makearray(0, [len(w), n])
        for i in range(n):
            for j in range(len(w)):
                rIn[j, i] = w[j]
        r2 = mycs.topixelmany(rIn)

        self.assertTrue(len(r2['numeric']) == len(rIn))
        for i in range(n):
            for j in range(len(w)):
                self.assertTrue(abs(p[j] - r2['numeric'][j, i]) < 1e-6)
        mycs.done()

    def test_naxes(self):
        """Test the naxes param with 0 or multiple axes"""
        mycs = cs.newcoordsys()
        n = mycs.naxes()

        self.assertTrue(n == 0)
        self.assertTrue(mycs.naxes() == 0)

        mycs = cs.newcoordsys(direction=True, spectral=True, stokes="I V", linear=2)

        self.assertTrue(mycs.naxes() == 6)

    def test_axesmap(self):
        """Test that maps are the same"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes="I V", linear=2)
        toworld = mycs.axesmap(toworld=True)
        topixel = mycs.axesmap(toworld=False)

        idx = range(len(mycs.referencepixel()['numeric']))

        self.assertTrue(np.all(toworld == idx))
        self.assertTrue(np.all(topixel == idx))
        mycs.done()

    def test_reorder(self):
        """Test that the coords are reordered"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes='I V', linear=1)
        order = [3, 2, 1, 0]
        mycs.reorder(order)

        self.assertTrue(mycs.coordinatetype(0) == ['Linear'])
        self.assertTrue(mycs.coordinatetype(1) == ['Spectral'])
        self.assertTrue(mycs.coordinatetype(2) == ['Stokes'])
        self.assertTrue(mycs.coordinatetype(3) == ['Direction'])

    def test_reorderInvalid(self):
        """Test that invalid inputs for reorder raises exceptions"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes='I V', linear=1)
        try:
            ok = mycs.reorder([1, 2])
        except Exception as e:
            ok = False
        self.assertFalse(ok)

        try:
            ok = mycs.reorder([1, 2, 3, 10])
        except Exception as e:
            ok = False
        self.assertFalse(ok)
        mycs.done()

    def test_frequencyVelocityBasic(self):
        """Test conversion between freq and velocity"""
        # It seems difficult to decouple these two tasks
        # Look into seperating these into their own test cases somehow?
        mycs, rv, restFreq, df, c, drv, freq, freqUnit = freqVelSetUp()

        vel = mycs.frequencytovelocity(value=freq, frequnit=freqUnit[0],
                                       doppler='radio', velunit='km/s')
        self.assertFalse(abs(vel) > 1e-6)

    def test_velocityToFrequency(self):
        """Test velocity To frequency conversion"""
        mycs, rv, restFreq, df, c, drv, freq, freqUnit = freqVelSetUp()
        vel = mycs.frequencytovelocity(value=freq, frequnit=freqUnit[0],
                                               doppler='radio', velunit='km/s')

        freq2 = mycs.velocitytofrequency(value=vel, frequnit=freqUnit[0],
                                         doppler='optical', velunit='km/s')
        self.assertFalse(abs(freq2-freq) > 1e-6)
        self.assertFalse(abs(vel) > 1e-6)
        #
        freq2 = mycs.velocitytofrequency(value=vel, frequnit=freqUnit[0],
                                         doppler='optical', velunit='km/s')
        self.assertFalse(abs(freq2 - freq) > 1e-6)
        ##

    def test_frequencyToVelocity(self):
        """Test frequency to velocity conversion"""
        mycs, rv, restFreq, df, c, drv, freq, freqUnit = freqVelSetUp()

        rp = mycs.referencepixel()['numeric']

        self.assertTrue(rp == 0)

        freq = mycs.toworld(value=rp + 1, format='n')
        vel = mycs.frequencytovelocity(value=list(freq['numeric']),
                                       frequnit=freqUnit[0],
                                       doppler='radio', velunit='m/s')
        d = abs(vel - (1000.0 * drv))
        self.assertFalse(d > 1e-6)

        freq2 = mycs.velocitytofrequency(value=vel, frequnit=freqUnit[0],
                                         doppler='radio', velunit='m/s')
        self.assertFalse(abs(freq2 - freq['numeric']) > 1e-6)
        ##

    def test_frequencyToVelocityDiff(self):
        """Test the velocity diff"""
        mycs, rv, restFreq, df, c, drv, freq, freqUnit = freqVelSetUp()

        freq = [rv['numeric'][0], freq]
        vel = mycs.frequencytovelocity(value=freq, frequnit=freqUnit[0],
                                       doppler='radio', velunit='m/s')
        self.assertTrue(len(vel == 2))

        d1 = abs(vel[0] - 0.0)
        d2 = abs(vel[1] - (1000.0 * drv))
        self.assertFalse(d1 > 1e-6 and d2 > 1e-6)

    def test_velocityToFrequencyDiff(self):
        """Test the drequency diff"""
        mycs, rv, restFreq, df, c, drv, freq, freqUnit = freqVelSetUp()
        freq = [rv['numeric'][0], freq]
        vel = mycs.frequencytovelocity(value=freq, frequnit=freqUnit[0],
                                       doppler='radio', velunit='km/s')

        freq2 = mycs.velocitytofrequency(value=vel, frequnit=freqUnit[0],
                                         doppler='radio', velunit='m/s')
        d1 = abs(freq[0] - freq2[0])
        d2 = abs(freq[1] - freq2[1])
        self.assertFalse(d1 > 1e-6 and d2 > 1e-6)

    def test_frequencyVelocityInvalidInput(self):
        """Test frequencytovelocity and velocitytofrequency with invalid inputs"""
        mycs = cs.newcoordsys(spectral=True)

        # Set rest freq to reference freq
        rv = mycs.referencevalue(format='n')
        restFreq = rv['numeric'][0]
        mycs.setrestfrequency(restFreq)

        # Forced errors
        try:
            vel = True
            vel = mycs.frequencytovelocity(value=rv['numeric'][0],
                                           frequnit='Jy',
                                           doppler='radio', velunit='km/s')
        except Exception as e:
            vel = False
        self.assertFalse(vel)

        try:
            freq = True
            freq = mycs.velocitytofrequency(value=rv['numeric'][0],
                                            frequnit='Jy',
                                            doppler='radio', velunit='km/s')
        except Exception as e:
            freq = False
        self.assertFalse(freq)
        ##
        try:
            vel = True
            vel = mycs.frequencytovelocity(value=rv['numeric'][0],
                                           frequnit='GHz',
                                           doppler='radio', velunit='doggies')
        except Exception as e:
            vel = False
        self.assertFalse(vel)

        try:
            freq = True
            freq = mycs.velocitytofrequency(value=rv['numeric'][0],
                                            frequnit='GHz',
                                            doppler='radio', velunit='doggies')
        except Exception as e:
            freq = False
            self.assertFalse(freq)
        #
        mycs.done()
        #
        mycs = cs.newcoordsys(direction=True, spectral=False)
        try:
            vel = True
            vel = mycs.frequencytovelocity(value=[1.0], frequnit='Hz',
                                           doppler='radio', velunit='km/s')
        except Exception as e:
            vel = False
        self.assertFalse(vel)
        mycs.done()

    def test_referenceLocation(self):
        """Test that setreferencelocation correctly sets ref value"""
        mycs = cs.newcoordsys(linear=2, spectral=True)
        p = [1.0, 1.0, 1.0]
        mycs.setreferencepixel(value=p)
        rp = mycs.referencepixel()['numeric']

        self.assertTrue(len(rp) == 3)
        self.assertTrue(np.all(np.isclose(rp, p, atol=1e-6)))

        inc = mycs.increment(format='n')['numeric']
        w = mycs.toworld([1, 1, 1], 'n')['numeric']
        w += inc
        p = [51, 51, 5]  # p = ((shp-1)/2.0) + 1
        mycs.setreferencelocation(pixel=p, world=w, mask=[True, True, True])
        rp = mycs.referencepixel()['numeric']
        rv = mycs.referencevalue(format='n')['numeric']

        self.assertTrue(abs(rv[0] - w[0]) < 1e-6 and abs(rv[1] - w[1]) < 1e-6, msg='setreferencelocation recovered wrong reference value')
        self.assertTrue(abs(rv[2] - w[2]) < 1e-6, msg='setreferencelocation recovered wrong reference value')

        self.assertTrue(abs(rp[0] - p[0]) < 1e-6 and abs(rp[1] - p[1]) < 1e-6, msg='setreferencelocation recovered wrong reference pixel')
        self.assertTrue(abs(rp[2] - p[2]) < 1e-6, msg='setreferencelocation recovered wrong reference pixel')
        mycs.done()

    def test_toAbsRelRefPix(self):
        """Test that converting from pixel coordinates gives the correct value"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes="I V LL", linear=2)
        p = mycs.referencepixel()
        pn = p['numeric']  # pointer to numeric part of p
        for i in range(len(pn)): pn[i] = pn[i] + 1
        p2 = mycs.torel(p)
        p3 = mycs.toabs(p2)['numeric']
        d = abs(p3 - pn)
        for i in range(len(d)):
            self.assertTrue(d[i] <= 1e-6)

    def test_toAbsRelWorld(self):
        """Test that converting from world coordinates gives the correct value"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes="I V LL", linear=2)
        p = mycs.referencepixel()['numeric']
        for i in range(len(p)): p[i] += 1
        #
        for f in ["n", "q", "s"]:
            w = mycs.toworld(p, format=f)
            w2 = mycs.torel(w)
            w3 = mycs.toabs(w2)

            p2 = mycs.topixel(w3)['numeric']
            self.assertTrue(np.all(np.isclose(p2, p, atol=1e-6)),
                            msg='torel/toabs world reflection test 1 failed for format "' + f + '"')

    def test_toAbsRelInvalidInput(self):
        """Test expected failure cases of toabs and torel"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes="I V LL", linear=2)

        p = mycs.referencepixel()
        try:
            p2 = mycs.toabs(p)
        except Exception as e:
            p2 = False
        self.assertFalse(p2)
        #
        p2 = mycs.torel(p)
        try:
            p3 = mycs.torel(p2)
        except Exception as e:
            p3 = False
        self.assertFalse(p3)
        #
        w = mycs.referencevalue()
        try:
            w2 = mycs.toabs(w)
        except:
            w2 = False
        self.assertFalse(w2)
        #
        w2 = mycs.torel(w)

        try:
            w3 = mycs.torel(w2)
        except Exception as e:
            w3 = False
        self.assertFalse(w3)

    def test_toAbsRelMany(self):
        """Test conversion of many to rel and abs"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes="I V LL", linear=2)
        p = mycs.referencepixel()['numeric']
        w = mycs.toworld(p, 'n')['numeric']
        n = 5
        pp = ia.makearray(0.0, [len(p), n])
        ww = ia.makearray(0.0, [len(w), n])
        print("w list: ", w)
        for i in range(n):
            for j in range(len(p)):
                pp[j, i] = p[i]
            for j in range(len(w)):
                ww[j, i] = w[i]
        print("ww list: ", ww)
        #
        relpix = mycs.torelmany(pp, False)
        abspix = mycs.toabsmany(relpix, False)
        #
        relworld = mycs.torelmany(ww, True)
        absworld = mycs.toabsmany(relworld, True)
        print("absworld list: ", absworld['numeric'])
        #

        self.assertTrue(len(relpix['numeric']) == len(p))
        self.assertTrue(len(abspix['numeric']) == len(relpix['numeric']))
        self.assertTrue(len(relworld['numeric']) == len(w))
        self.assertTrue(len(absworld['numeric']) == len(relworld['numeric']))
        for i in range(n):
            for j in range(len(p)):
                self.assertTrue(abs(p[j] - abspix['numeric'][j, i]) < 1e-6,
                                msg='toabsmany/torelmany gives wrong values for pixels')
            for j in range(len(w)):
                print((w[j], absworld['numeric'][j,i]))
                self.assertTrue(alleq(w[j],absworld['numeric'][j,i],1e-6))
            #
        mycs.done()

    def test_convert(self):
        """Test the conversion from abs pix to rel pix"""
        mycs = cs.newcoordsys(direction=True, spectral=True, stokes="I V LL",
                              linear=2)
        tol = 1.0e-6
        n = mycs.naxes()

        absin = n * [True]
        unitsin = n * ['pix']
        coordin = mycs.referencepixel()['numeric']  # Make sure in range of stokes
        for i in range(len(coordin)):
            coordin[i] += 2
        unitsout = n * ['pix']
        dopplerin = 'radio'
        dopplerout = 'radio'
        #
        absout = n * [False]
        p = mycs.convert(list(coordin), absin, dopplerin, unitsin,
                         absout, dopplerout, unitsout)
        #
        p2 = mycs.torel(coordin, False)['numeric']

        self.assertTrue(len(p2) == n)
        self.assertTrue(np.all(np.isclose(p, p2, atol=tol)))

    def test_convertMany(self):

        mycs = cs.newcoordsys(direction=True, spectral=True, stokes="I V LL",
                              linear=2)
        tol = 1.0e-6
        n = mycs.naxes()

        coordin = mycs.referencepixel()['numeric']
        absin = n * [True]
        unitsin = n * ['pix']
        dopplerin = 'radio'
        absout = n * [True]
        unitsout = mycs.units()
        dopplerout = 'radio'
        #
        coordout = mycs.convert(list(coordin), absin, dopplerin,
                                unitsin, absout, dopplerout, unitsout)
        #
        rIn = ia.makearray(0, [len(coordin), 10])
        for i in range(10):
            for j in range(len(coordin)):
                rIn[j, i] = coordin[j]
        rOut = mycs.convertmany(rIn, absin, dopplerin, unitsin,
                                absout, dopplerout, unitsout)
        for i in range(10):
            for j in range(len(coordin)):
                d = rOut[j, i] - coordout[j]
                self.assertTrue(d < tol)

    def test_setSpectralInvalidInput(self):

        mycs = cs.newcoordsys(direction=True)
        try:
            ok = mycs.setspectral(refcode='lsrk')
        except Exception as e:
            ok = False
        self.assertFalse(ok)
        mycs.done()

    def test_setSpectral(self):
        #
        mycs = cs.newcoordsys(spectral=True)
        #
        rc = 'LSRK'
        mycs.setspectral(refcode=rc)
        #
        rf = qa.quantity('1.0GHz')
        mycs.setspectral(restfreq=rf)
        rf2 = mycs.restfrequency()
        rf3 = qa.convert(rf2, 'GHz')
        #
        fd = [1, 1.5, 2, 2.5, 3]
        fq = qa.quantity(fd, 'GHz')
        mycs.setspectral(frequencies=fq)
        #
        doppler = 'optical'
        vunit = 'km/s'
        vd = mycs.frequencytovelocity(fd, 'GHz', doppler, vunit)
        vq = qa.quantity(vd, vunit)
        mycs.setspectral(velocities=vq, doppler=doppler)
        #
        fd2 = mycs.velocitytofrequency(vd, 'GHz', doppler, vunit)

        self.assertTrue(mycs.referencecode('spectral') == [rc])
        self.assertTrue(qa.getvalue(rf3) == 1.0)
        self.assertTrue(np.all(np.isclose(fd2, fd, atol=1e-6)),
                        msg='setspectral/freq/vel consistency test failed')
        mycs.done()

    def test_setTabular(self):
        """Test that with tabular coordinates reference and pixel values can be set"""
        mycs = cs.newcoordsys(tabular=True)
        #
        p = [0, 1, 2, 3, 4]
        w = [10, 20, 30, 40, 50]
        mycs.settabular(pixel=p, world=w)
        rv = mycs.referencevalue()['numeric']
        rp = mycs.referencepixel()['numeric']

        self.assertTrue(rv[0] == w[0])
        self.assertTrue(rp == 0.0)
        self.assertTrue(rp == p[0])
        #

    def test_setTabularIncorrect(self):
        """Test that incorrect inputs will raise exceptions"""
        mycs = cs.newcoordsys(direction=True)

        try:
            ok = mycs.settabular(pixel=[1, 2], world=[1, 2])
        except Exception as e:
            ok = False
        self.assertFalse(ok)

        mycs.done()
        mycs = cs.newcoordsys(tabular=True)
        try:
            ok = mycs.settabular(pixel=[0, 1, 2], world=[10, 20])
        except Exception as e:
            ok = False
        self.assertFalse(ok)
        #
        try:
            ok = mycs.settabular(pixel=[0, 1], world=[1, 10, 20])
        except Exception as e:
            ok = False
        self.assertFalse(ok)
        #
        ok = mycs.settabular(pixel=[0, 1, 2], world=[1, 10, 20])
        try:
            ok = mycs.settabular(pixel=[0, 1, 2, 3])
        except Exception as e:
            ok = False
        self.assertFalse(ok)
        try:
            ok = mycs.settabular(world=[0, 1, 2, 3])
        except Exception as e:
            ok = False
        self.assertFalse(ok)
        #
        mycs.done()

    def test_addCoordinate(self):
        """Test that coordinates can be added"""
        mycs = cs.newcoordsys()
        mycs.addcoordinate(direction=True, spectral=True, linear=2, tabular=True, stokes="I V")
        n = mycs.ncoordinates()

        # We don't know what order they will be in.
        types = mycs.coordinatetype()
        hasDir = False
        hasSpec = False
        hasLin = False
        hasTab = False
        hasStokes = False
        for i in range(n):
            if (types[i] == 'Direction'):
                hasDir = True
            elif (types[i] == 'Spectral'):
                hasSpec = True
            elif (types[i] == 'Linear'):
                hasLin = True
            elif (types[i] == 'Tabular'):
                hasTab = True
            elif (types[i] == 'Stokes'):
                hasStokes = True
        #
        ok = hasDir and hasSpec and hasLin and hasTab and hasStokes
        self.assertTrue(n == 5)
        self.assertTrue(ok)
        #
        mycs.done()

    def test_referenceConvert(self):
        """Set values with reference conversion"""
        mycs = cs.newcoordsys(direction=True, spectral=True)
        v = mycs.units()
        v[0] = 'rad'
        v[1] = 'rad'
        v[2] = 'Hz'
        mycs.setunits(v)
        #
        mycs.setrestfrequency(1.420405752E9)
        #
        mycs.setreferencecode(value='J2000', type='direction', adjust=False)
        mycs.setreferencecode(value='LSRK', type='spectral', adjust=False)
        #
        v = mycs.referencevalue()['numeric']
        v[0] = 0.0
        v[1] = -0.5
        v[2] = 1.4e9
        mycs.setreferencevalue(v)
        #
        v = list(mycs.referencepixel()['numeric'])
        v[0] = 101
        v[1] = 121
        v[2] = 10.5
        mycs.setreferencepixel(v)
        #
        v = mycs.increment()['numeric']
        v[0] = -1.0e-6
        v[1] = 2.0e-6
        v[2] = 4.0e6
        mycs.setincrement(v)
        #
        v = mycs.units()
        v[0] = 'deg'
        v[1] = 'deg'
        mycs.setunits(v)

        mycs.setconversiontype(direction='GALACTIC', spectral='BARY')
        # local d,s
        d = mycs.conversiontype(type='direction')
        s = mycs.conversiontype(type='spectral')
        #
        p = mycs.referencepixel()['numeric']
        for i in range(len(p)): p[i] += 10.0
        w = mycs.toworld(value=p, format='n')
        p2 = mycs.topixel(value=w)['numeric']
        #
        # Need to look into why i need such a large tolerance
        #
        tol = 1e-3
        self.assertTrue(d == 'GALACTIC' and s == 'BARY')
        self.assertTrue(len(p) == 3)
        self.assertTrue(len(p2) == 3)
        self.assertTrue(np.all(np.isclose(p2, p, tol)))

    def test_setDirectionPixVal(self):
        """Test that direction is set correctly with ref pix and val"""
        mycs = cs.newcoordsys(direction=True)

        refcode = 'GALACTIC'
        proj = 'CAR'
        projpar = []
        refpix = list(mycs.referencepixel()['numeric'])
        for item in refpix:
            item *= 1.1
        refval = mycs.referencevalue(format='n')['numeric']
        for i in range(len(refval)): refval[i] *= 1.1
        xform = ia.makearray(0.0, [2, 2])
        xform[0, 0] = 1.0
        xform[1, 1] = 1.0
        mycs.setdirection(refcode=refcode,
                          proj=proj, projpar=projpar,
                          refpix=refpix, refval=refval,
                          xform=xform)
        #
        self.assertTrue(proj == mycs.projection()['type'])
        if projpar:
            self.assertTrue(projpar == mycs.projection()['parameters'])
        self.assertTrue(np.all(np.isclose(refpix, mycs.referencepixel()['numeric'], atol=1e-6)))
        self.assertTrue(np.all(np.isclose(refval, mycs.referencevalue(format='n')['numeric'], atol=1e-6)))

    def test_setDirectionVal(self):
        """Test that direction is set correctly with ref val"""
        mycs = cs.newcoordsys(direction=True)
        refcode = 'J2000'
        proj = 'SIN'
        projpar = [0, 0]
        refval = "20.0deg -33deg"
        refval2 = [20, -33]
        mycs.setdirection(refcode=refcode,
                               proj=proj, projpar=projpar,
                               refval=refval)
        #
        self.assertTrue(proj == mycs.projection()['type'])
        self.assertTrue(np.all(np.isclose(projpar, mycs.projection()['parameters'], atol=1e-6)))
        mycs.setunits(value="deg deg")
        self.assertTrue(np.all(np.isclose(refval2, mycs.referencevalue(format='n')['numeric'], atol=1e-6)))

    def test_replace(self):
        """Test that a coord types can be replaced"""
        mycs = cs.newcoordsys(direction=True, linear=1)
        cs2 = cs.newcoordsys(spectral=True)
        mycs.replace(cs2.torecord(), whichin=0, whichout=1)

        self.assertTrue(mycs.coordinatetype(1) == ['Spectral'])

    def test_replaceIncorrect(self):
        """Test that incorrect inputs for replace result in failures"""
        mycs = cs.newcoordsys(direction=True, linear=1)
        cs2 = cs.newcoordsys(linear=1)

        try:
            ok = mycs.replace(cs2.torecord(), whichin=0, whichout=0)
        except Exception as e:
            ok = False
        self.assertFalse(ok)


if __name__ == '__main__':
    unittest.main()
