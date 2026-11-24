###########################################################################
# test_task_plotbandpass.py
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
# https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.visualization.plotbandpass.html
#
##########################################################################

import os
import struct
import unittest

from casatasks import plotbandpass
from casatools import ctsys

datapath = ctsys.resolve('unittest/plotbandpass/')
figdir= os.getcwd() + '/'
# Set to False to Leave PNGs and PDFs
delete_artifacts = True

def pngWidthHeight(filename):
    """
    Reads the width and height of a png image (in pixels).
    -Todd Hunter
    """
    if (os.path.exists(filename) == False):
        print("Cannot find file = ", filename)
        return Exception("{} not Found".format(filename)), False

    f = open(filename, 'rb')
    data = f.read()
    f.close()

    if (data[12:16].decode("utf-8") == 'IHDR'):
        w, h = struct.unpack('>LL', data[16:24])
        width = int(w)
        height = int(h)

    return width, height


class plotbandpass_1_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        os.symlink(datapath+'Band7multi_april22.ms', os.getcwd() + '/Band7multi_april22.ms')
        os.symlink(datapath+'bandpass.bcal', os.getcwd() + '/bandpass.bcal')

    def tearDown(self):
        os.unlink(os.getcwd() + '/Band7multi_april22.ms')
        os.unlink(os.getcwd() + '/bandpass.bcal')
        if delete_artifacts:
            artifacts = os.listdir(figdir)
            for artifact in artifacts:
                if artifact.endswith(".pdf") or artifact.endswith(".png"):
                    os.remove(artifact)

    @classmethod
    def tearDownClass(cls):
        pass

    # 0
    def test_createImage_regression00(self):
        '''test_plotbandpass: test_createImage_regression00'''
        #regression00.pdf
        #regression00.spw00.t00.png
        #regression00.spw01.t01.png
        #regression00.spw02.t02.png

        plotbandpass(datapath + 'bandpass.bcal',showtsky=False,xaxis='freq',yaxis='amp',overlay='antenna',spw='',field='0', interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(0),debug=False)

        width,height = pngWidthHeight(os.getcwd()+'/regression00.spw02.t02.png')
        self.assertTrue(width == 864,"Observed: {}, Expected: {}".format(width, 864) )

    #1
    def test_createImage_regression01(self):
        '''test_plotbandpass: test_createImage_regression01'''
        #regression01.DV04.spw00.t00.png
        #regression01.DV07.spw00.t01.png
        #regression01.DV08.spw00.t02.png
        #regression01.DV10.spw00.t00.png
        #regression01.pdf
        #regression01.PM01.spw00.t01.png
        #regression01.PM02.spw00.t02.png

        plotbandpass(datapath+'bandpass.bcal',showtsky=False,xaxis='freq',yaxis='amp',overlay='baseband',spw='',field='3c279', interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(1),debug=False)

        width,height = pngWidthHeight(os.getcwd()+'/regression01.DV04.spw00.t00.png')
        self.assertTrue(width == 864,"Observed: {}, Expected: {}".format(width, 864) )

    # 2
    def test_createImage_regression02(self):
        '''test_plotbandpass: test_createImage_regression02'''
        #regression02.pdf
        #regression02.spw00.t00.png
        #regression02.spw01.t01.png
        #regression02.spw02.t02.png

        plotbandpass(datapath+'bandpass.bcal',showtsky=False,xaxis='freq',yaxis='ampdb',overlay='antenna',spw='',
             field='!Titan,!TW Hya,!J1147-382=QSO',
             interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(2),debug=False)

        width,height = pngWidthHeight(os.getcwd()+'/regression02.spw00.t00.png')
        self.assertTrue(width == 864,"Observed: {}, Expected: {}".format(width, 864) )

    # 3
    def test_createImage_regression03(self):
        '''test_plotbandpass: test_createImage_regression03'''
        """
        regression03.DV04.spw00.t00.png
        regression03.DV06.spw00.t02.png
        regression03.DV06.spw01.t01.png
        regression03.DV06.spw02.t00.png
        regression03.DV06.spw02.t02.png
        regression03.DV06.spw03.t01.png
        regression03.DV07.spw00.t00.png
        regression03.DV07.spw00.t02.png
        regression03.DV07.spw01.t01.png
        regression03.DV07.spw02.t00.png
        regression03.DV07.spw02.t02.png
        regression03.DV07.spw03.t01.png
        regression03.DV08.spw00.t00.png
        regression03.DV08.spw00.t02.png
        regression03.DV08.spw01.t01.png
        regression03.DV08.spw02.t00.png
        regression03.DV08.spw02.t02.png
        regression03.DV08.spw03.t01.png
        regression03.DV09.spw00.t00.png
        regression03.DV09.spw00.t02.png
        regression03.DV09.spw01.t01.png
        regression03.DV09.spw02.t00.png
        regression03.DV09.spw02.t02.png
        regression03.DV09.spw03.t01.png
        regression03.DV10.spw00.t00.png
        regression03.DV10.spw00.t02.png
        regression03.DV10.spw01.t01.png
        regression03.DV10.spw02.t00.png
        regression03.DV10.spw02.t02.png
        regression03.DV10.spw03.t01.png
        regression03.pdf
        regression03.PM01.spw00.t00.png
        regression03.PM01.spw00.t02.png
        regression03.PM01.spw01.t01.png
        regression03.PM01.spw02.t00.png
        regression03.PM01.spw02.t02.png
        regression03.PM01.spw03.t01.png
        regression03.PM02.spw00.t00.png
        regression03.PM02.spw00.t02.png
        regression03.PM02.spw01.t01.png
        regression03.PM02.spw02.t00.png
        regression03.PM02.spw02.t02.png
        regression03.PM02.spw03.t01.png
        regression03.PM03.spw00.t00.png
        regression03.PM03.spw00.t02.png
        regression03.PM03.spw01.t01.png
        regression03.PM03.spw02.t00.png
        regression03.PM03.spw03.t02.png
        """
        plotbandpass(datapath+'bandpass.bcal',showtsky=False,xaxis='freq',yaxis='both',phase=[-180,180],plotrange=[0,0,0,2],
                  interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(3),debug=False)

        width,height = pngWidthHeight(os.getcwd()+'/regression03.DV04.spw00.t00.png')
        self.assertTrue(width == 864,"Observed: {}, Expected: {}".format(width, 864) )

    # 4
    def test_createImage_regression04(self):
        '''test_plotbandpass: test_createImage_regression04'''
        #regression04.pdf
        #regression04.spw00.t00.png
        #regression04.spw01.t01.png
        #regression04.spw02.t02.png

        plotbandpass(datapath+'bandpass.bcal',showtsky=False,xaxis='freq',yaxis='amp',overlay='antenna',spw='',field='',
             chanrange='1200~2000',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(4),
             debug=False, chanrangeSetXrange=True)

        width,height = pngWidthHeight(os.getcwd()+'/regression04.spw00.t00.png')
        self.assertTrue(width == 864,"Observed: {}, Expected: {}".format(width, 864) )

    # 5
    def test_createImage_regression05(self):
        '''test_plotbandpass: test_createImage_regression05'''
        plotbandpass(datapath+'bandpass.bcal',showatm=True,xaxis='chan',yaxis='amp',overlay='antenna',spw='',field='',
                  plotrange=[1200,2000,0,0],interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(5),debug=False)

        width,height = pngWidthHeight(os.getcwd()+'/regression05.spw00.t00.png')
        self.assertTrue(width == 864,"Observed: {}, Expected: {}".format(width, 864) )

    # 6
    def test_createImage_regression06(self):
        '''test_plotbandpass: test_createImage_regression06'''
        plotbandpass(datapath+'bandpass.bcal',showatm=True,xaxis='chan',yaxis='amp',spw='',field='',plotrange=[1200,3840,0,0],
                  interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(6),debug=False)

        width,height = pngWidthHeight(os.getcwd()+'/regression06.DV04.spw00.t00.png')
        self.assertTrue(width == 864,"Observed: {}, Expected: {}".format(width, 864) )

    # 7
    def test_createImage_regression07(self):
        '''test_plotbandpass: test_createImage_regression07'''
        plotbandpass(datapath+'bandpass.bcal',showtsky=False,xaxis='chan',yaxis='amp',overlay='antenna',spw='',field='',showatm=True,
                  interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(7),debug=False)

        width,height = pngWidthHeight(os.getcwd()+'/regression07.spw00.t00.png')
        self.assertTrue(width == 864,"Observed: {}, Expected: {}".format(width, 864) )

    # 8
    def test_createImage_regression08(self):
        '''test_plotbandpass: test_createImage_regression08'''
        plotbandpass(datapath+'bandpass.bcal',showtsky=False,xaxis='freq',yaxis='phase',overlay='antenna',spw='',field='',
                  interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(8),debug=False)

        width,height = pngWidthHeight(os.getcwd()+'/regression08.spw00.t00.png')
        self.assertTrue(width == 864,"Observed: {}, Expected: {}".format(width, 864) )

    # 9
    def test_createImage_regression09(self):
        '''test_plotbandpass: test_createImage_regression09'''
        plotbandpass(datapath+'bandpass.bcal',showtsky=False,xaxis='freq',yaxis='phase',overlay='antenna',spw='',field='',
             chanrange='1200~1800',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(9),
             debug=False, chanrangeSetXrange=True)

        width,height = pngWidthHeight(os.getcwd()+'/regression09.spw00.t00.png')
        self.assertTrue(width == 864,"Observed: {}, Expected: {}".format(width, 864) )

    # 10
    def test_createImage_regression10(self):
        '''test_plotbandpass: test_createImage_regression10'''
        plotbandpass(datapath+'bandpass.bcal',overlay='antenna',yaxis='amp',field='0~1,4',xaxis='freq',showtsky=True,
                  interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(10),debug=False)

        width,height = pngWidthHeight(os.getcwd()+'/regression10.spw00.t00.png')
        self.assertTrue(width == 864,"Observed: {}, Expected: {}".format(width, 864) )

# class plotbandpass_2_test(unittest.TestCase):

#     @classmethod
#     def setUpClass(cls):
#         pass

#     def setUp(self):
#         os.symlink(datapath+'X3c1.ms', os.getcwd() + '/X3c1.ms')

#     def tearDown(self):
#         os.unlink(os.getcwd() + '/X3c1.ms')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)


#     @classmethod
#     def tearDownClass(cls):
#         pass

#     #11
#     @unittest.skip("N/A")
#     def test_b_skipspw19high_regression11(self):
#         '''test_plotbandpass: test_b_skipspw19high_regression11'''
#         plotbandpass(datapath + 'bandpass_b_skipspw19high.bcal',yaxis='amp',field='0',xaxis='freq',
#                   caltable2=datapath + 'bandpass_bpoly_skipspw19high.bcal',showpoints=True,spw='0,1',
#                   interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(11),debug=False)

#     # 12
#     @unittest.skip("N/A")
#     def test_b_skipspw19high_regression12(self):
#         '''test_plotbandpass: test_b_skipspw19high_regression12'''
#         plotbandpass(datapath +'bandpass_b_skipspw19high.bcal',yaxis='phase',field='0',xaxis='freq',
#                   caltable2= datapath + 'bandpass_bpoly_skipspw19high.bcal',showpoints=True,spw=0,
#                   caltable3= datapath + 'bandpass_bpoly_skipspw19high.bcal',interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(12),debug=False)


#     # 13
#     @unittest.skip("N/A")
#     def test_b_skipspw19high_regression13(self):
#         '''test_plotbandpass: test_b_skipspw19high_regression13'''
#         plotbandpass(datapath + 'bandpass_b_skipspw19high.bcal',yaxis='both',field='0',xaxis='freq',
#                   caltable2= datapath + 'bandpass_bpoly_skipspw19high.bcal',showpoints=True,spw=0,
#                   caltable3= datapath + 'bandpass_bpoly_skipspw19high.bcal',interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(13),debug=False)

# class plotbandpass_X3c1_test(unittest.TestCase):

#     @classmethod
#     def setUpClass(cls):
#         pass

#     def setUp(self):
#         os.symlink(datapath+'X3c1.ms', os.getcwd() + '/X3c1.ms')

#     def tearDown(self):
#         os.unlink(os.getcwd() + '/X3c1.ms')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)


#     @classmethod
#     def tearDownClass(cls):
#         pass

#     #14
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_fdm_regression14(self):
#         '''test_plotbandpass: test_X3c1_tsys_fdm_regression14'''
#         plotbandpass(datapath + 'X3c1.tsys.fdm',overlay='antenna',yaxis='amp',field='1',xaxis='chan',
#                   showtsky=True,interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(14))

#     # 15
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_fdm_regression15(self):
#         '''test_plotbandpass: test_X3c1_tsys_fdm_regression15'''
#         plotbandpass(datapath + 'X3c1.tsys.fdm',overlay='antenna',yaxis='amp',field='1',xaxis='chan',
#                   poln='y',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(15),debug=False)


#     # 16
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression16(self):
#         '''test_plotbandpass: test_X3c1_tsys_fdm_regression16'''
#         plotbandpass(datapath + 'X3c1.tsys',overlay='antenna',yaxis='amp',field='0~1,4',xaxis='chan',
#                   interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(16),debug=False)

#     # 17
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression17(self):
#         '''test_plotbandpass: test_X3c1_tsys_fdm_regression17'''
#         plotbandpass(datapath + 'X3c1.tsys',overlay='time',yaxis='amp',field='2',xaxis='chan',interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(17),debug=False)

#     # 18
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression18(self):
#         '''test_plotbandpass: test_X3c1_tsys_fdm_regression18'''
#         plotbandpass(datapath + 'X3c1.tsys',overlay='',yaxis='amp',field='',xaxis='freq',showfdm=True,interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(18),debug=False)

#     # 19
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression19(self):
#         '''test_plotbandpass: test_X3c1_tsys_fdm_regression19'''
#         plotbandpass(datapath + 'X3c1.tsys',overlay='time',yaxis='amp',field='',xaxis='freq',showfdm=True,interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(19),debug=False)

#     # 20
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression20(self):
#         '''test_plotbandpass: test_X3c1_tsys_fdm_regression20'''
#         plotbandpass(datapath + 'X3c1.tsys',overlay='',yaxis='amp',field='2',xaxis='freq',chanrange='45~65',
#              interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(20),debug=False,
#              chanrangeSetXrange=True)

# class plotbandpass_smooth_test(unittest.TestCase):

#     @classmethod
#     def setUpClass(cls):
#         pass

#     def setUp(self):
#         os.symlink(datapath+'Band7multi_april22.ms', os.getcwd() + '/Band7multi_april22.ms')

#     def tearDown(self):
#         os.unlink(os.getcwd() + '/Band7multi_april22.ms')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)

#     @classmethod
#     def tearDownClass(cls):
#         pass

#     # 21
#     @unittest.skip("N/A")
#     def test_b_skipspw19high_regression21(self):
#         '''test_plotbandpass: test_b_skipspw19high_regression21'''
#         plotbandpass(datapath + 'bandpass_bpoly_skipspw19high.bcal',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(21),debug=False,xaxis='freq')


#     # 22
#     @unittest.skip("N/A")
#     def test_b_smooth_regression22(self):
#         '''test_plotbandpass: test_b_smooth_regression22'''
#         plotbandpass(datapath +'bandpass.bcal',caltable2=datapath +'bandpass.bcal_smooth',xaxis='freq',interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(22),debug=False)

#     # 23
#     @unittest.skip("N/A")
#     def test_b_smooth_regression23(self):
#         '''test_plotbandpass: test_b_smooth_regression23'''
#         plotbandpass(datapath +'bandpass.bcal',caltable2=datapath +'bandpass.bcal_smooth',xaxis='freq',yaxis='ampdb',
#                   interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(23),debug=False)

#     # 24
#     @unittest.skip("N/A")
#     def test_b_smooth_regression24(self):
#         '''test_plotbandpass: test_b_smooth_regression24'''
#         plotbandpass(datapath +'bandpass.bcal',caltable2=datapath +'bandpass.bcal_smooth',yaxis='phase', xaxis='freq',
#                   interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(24),debug=False)

#     # 25
#     @unittest.skip("N/A")
#     def test_b_smooth_regression25(self):
#         '''test_plotbandpass: test_b_smooth_regression25'''
#         plotbandpass(datapath +'bandpass.bcal',caltable2=datapath +'bandpass.bcal_smooth',xaxis='freq',chanrange='1000~3000',
#              interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(25),debug=False,
#              chanrangeSetXrange=True)

#     # 26
#     @unittest.skip("N/A")
#     def test_b_smooth_regression26(self):
#         '''test_plotbandpass: test_b_smooth_regression26'''
#         plotbandpass(datapath +'bandpass.bcal',caltable2=datapath +'bandpass.bcal_smooth',xaxis='freq',showtsky=True,
#                   interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(26),debug=False)

#     # 27
#     @unittest.skip("N/A")
#     def test_b_smooth_regression27(self):
#         '''test_plotbandpass: test_b_smooth_regression27'''
#         plotbandpass(datapath +'bandpass.bcal',caltable2=datapath +'bandpass.bcal_smooth',xaxis='freq',poln='x',
#                   showflagged=True,interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(27),debug=False)

#     # 28
#     @unittest.skip("N/A")
#     def test_b_smooth_regression28(self):
#         '''test_plotbandpass: test_b_smooth_regression28'''
#         plotbandpass(datapath +'bandpass.bcal',caltable2=datapath +'bandpass.bcal_smooth',xaxis='freq',poln='x',
#                   showflagged=True, showtsky=True,interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(28),debug=False)

# class plotbandpass_X3c1_2_test(unittest.TestCase):

#     @classmethod
#     def setUpClass(cls):
#         pass

#     def setUp(self):
#         os.symlink(datapath+'X3c1.ms', os.getcwd() + '/X3c1.ms')

#     def tearDown(self):
#         os.unlink(os.getcwd() + '/X3c1.ms')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)

#     #29
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression29(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression29'''
#         plotbandpass(caltable=datapath +'X3c1.tsys.fdm',caltable2=datapath +'X3c1.tsys',    yaxis='amp',xaxis='freq',
#                   interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(29),debug=False)

#     # 30
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression30(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression30'''
#         plotbandpass(caltable=datapath +'X3c1.tsys.fdm',caltable2=datapath +'X3c1.tsys',    yaxis='amp',xaxis='freq',
#                   showtsky=True, interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(30),debug=False)

#     # 31
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression31(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression31'''
#         plotbandpass(caltable=datapath +'X3c1.tsys',    caltable2=datapath +'X3c1.tsys.fdm',yaxis='amp',xaxis='freq',
#                   interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(31),debug=False)

#     # 32
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression32(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression32'''
#         plotbandpass(caltable=datapath +'X3c1.tsys',    caltable2=datapath +'X3c1.tsys.fdm',yaxis='amp',xaxis='freq',
#                   showtsky=True,interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(32),debug=False)

#     # 33
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression33(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression33'''
#         plotbandpass(caltable=datapath +'X3c1.tsys',caltable2=datapath +'X3c1.tsys.fdm',    yaxis='both',xaxis='freq',
#              chanrange='10~118',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(33),
#              debug=False, chanrangeSetXrange=True)

#     # 34
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression34(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression34'''
#         plotbandpass(caltable=datapath +'X3c1.tsys.fdm',caltable2=datapath +'X3c1.tsys',    yaxis='amp',xaxis='freq',
#              chanrange='1000~3000',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(34),
#              debug=False, chanrangeSetXrange=True)

#     # 35
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression35(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression35'''
#         plotbandpass(caltable=datapath +'X3c1.tsys.fdm',caltable2=datapath +'X3c1.tsys',    yaxis='amp',xaxis='freq',
#              chanrange='1000~3000',showtsky=True,interactive=False,buildpdf=True,
#              figfile=figdir+'regression%02d'%(35),debug=False, chanrangeSetXrange=True)

#     # 36
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression36(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression36'''
#         plotbandpass(caltable=datapath +'X3c1.tsys',    caltable2=datapath +'X3c1.tsys.fdm',yaxis='both',xaxis='freq',
#                   interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(36),debug=False)

#     # 37
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression37(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression37'''
#         plotbandpass(caltable=datapath +'X3c1.tsys',    caltable2=datapath +'X3c1.tsys.fdm',yaxis='both',xaxis='freq',
#                   showtsky=True,interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(37),debug=False)

#     # 38
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression38(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression38'''
#         plotbandpass(caltable=datapath +'X3c1.tsys.fdm',caltable2=datapath +'X3c1.tsys',    yaxis='both',xaxis='freq',
#                   zoom='intersect',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(38),debug=False)

#     # 39
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression39(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression39'''
#         plotbandpass(caltable=datapath +'X3c1.tsys',    caltable2=datapath +'X3c1.tsys.fdm',yaxis='both',xaxis='freq',
#                   zoom='intersect',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(39),debug=False)

#     # 40
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression40(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression40'''
#         plotbandpass(caltable=datapath +'X3c1.tsys.fdm',caltable2=datapath +'X3c1.tsys',yaxis='amp',xaxis='freq',poln='XX',
#                   interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(40),debug=False)

#     # 41
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression41(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression41'''
#         plotbandpass(caltable=datapath +'X3c1.tsys.fdm',caltable2=datapath +'X3c1.tsys',yaxis='amp',field='1',xaxis='freq',
#                   poln='YY',zoom='intersect',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(41),debug=False)

#     # 42
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression42(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression42'''
#         plotbandpass(caltable=datapath +'X3c1.tsys.fdm',caltable2=datapath +'X3c1.tsys',yaxis='amp',field='1',xaxis='freq',
#                   poln='YY',zoom='intersect',showatm=True,interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(42),debug=False)

#     # 43
#     @unittest.skip("N/A")
#     def test_X3c1_tsys_regression43(self):
#         '''test_plotbandpass: test_X3c1_tsys_regression43'''
#         plotbandpass(caltable=datapath +'X3c1.tsys.fdm',yaxis='amp',field='1',xaxis='chan',poln='YY',zoom='intersect',
#                   showatm=True,showimage=True,interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(43),debug=False)

# class plotbandpass_multi_field_bandpass_test(unittest.TestCase):

#     @classmethod
#     def setUpClass(cls):
#         pass

#     def setUp(self):
#         os.symlink(datapath+'Band7multi_april22.ms', os.getcwd() + '/Band7multi_april22.ms')

#     def tearDown(self):
#         os.unlink(os.getcwd() + '/Band7multi_april22.ms')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)


#     #tests for multi-field bandpass solution overlay: in SciVer/TWHya
#     # 44
#     @unittest.skip("N/A")
#     def test_multi_field_bandpass_regression44(self):
#         '''test_plotbandpass: test_multi_field_bandpass_regression44'''
#         plotbandpass(datapath + 'band7multi_a6p7_titan.bcal',caltable2=datapath +'band7multi_b.bcal',xaxis='freq',
#              yaxis='amp',chanrange='',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(44),
#              debug=False)

#     # 45
#     @unittest.skip("N/A")
#     def test_multi_field_bandpass_regression45(self):
#         '''test_plotbandpass: test_multi_field_bandpass_regression45'''
#         plotbandpass(datapath +'band7multi_a6p7_titan.bcal',caltable2=datapath +'band7multi_b.bcal',xaxis='freq',
#              yaxis='amp',chanrange='',showflagged=True,interactive=False,buildpdf=True,
#              figfile=figdir+'regression%02d'%(45),debug=False)

#     # 46
#     @unittest.skip("N/A")
#     def test_multi_field_bandpass_regression46(self):
#         '''test_plotbandpass: test_multi_field_bandpass_regression46'''
#         plotbandpass(caltable=datapath +'band7multi_b.bcal',caltable3=datapath +'band7multi_bpoly_a6p7_titan.bcal',
#                   caltable2=datapath +'band7multi_bpoly.bcal',xaxis='freq',yaxis='both',interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(46),debug=False)


# class plotbandpass_multi_field_Tsys_solution_overlay_test(unittest.TestCase):

#     @classmethod
#     def setUpClass(cls):
#         pass

#     def setUp(self):
#         os.symlink(datapath+'2011.0.00150.S/uid___A002_X54d35d_X761.ms.tsys', os.getcwd() + '/uid___A002_X54d35d_X761.ms.tsys')
#         os.symlink(datapath+'2011.0.00150.S/uid___A002_X54d35d_X761.ms', os.getcwd() + '/uid___A002_X54d35d_X761.ms')

#     def tearDown(self):
#         os.unlink(os.getcwd() + '/uid___A002_X54d35d_X761.ms')
#         os.unlink(os.getcwd() + '/uid___A002_X54d35d_X761.ms.tsys')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)


#     # 47
#     @unittest.skip("N/A")
#     def test_multi_field_bandpass_regression47(self):
#         '''test_plotbandpass: test_multi_field_bandpass_regression47'''
#         #tests for multi-field Tsys solution overlay (new-style cal tables):

#         plotbandpass('uid___A002_X54d35d_X761.ms.tsys', overlay='time', xaxis='freq', yaxis='amp', subplot=22,interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(47),debug=False)

#     # 48
#     @unittest.skip("N/A")
#     def test_multi_field_bandpass_regression48(self):
#         '''test_plotbandpass: test_multi_field_bandpass_regression48'''
#         plotbandpass('uid___A002_X54d35d_X761.ms.tsys', overlay='antenna', xaxis='freq', yaxis='amp', subplot=22,interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(48),debug=False)

#     # 49
#     @unittest.skip("N/A")
#     def test_multi_field_bandpass_regression49(self):
#         '''test_plotbandpass: test_multi_field_bandpass_regression49'''
#         plotbandpass('uid___A002_X54d35d_X761.ms.tsys', overlay='time', timeranges='0,1', interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(49),debug=False)

#     # 50   # plot did not complete in au, but did in task on Apr02, sever error on Apr05
#     @unittest.skip("N/A")
#     def test_multi_field_bandpass_regression50(self):
#         '''test_plotbandpass: test_multi_field_bandpass_regression50'''
#         plotbandpass('uid___A002_X54d35d_X761.ms.tsys', overlay='time', scans='3,6', interactive=False, buildpdf=True,figfile=figdir+'regression%02d'%(50),antenna='0,1',debug=False)

#     # 82
#     @unittest.skip("N/A")
#     def test_multi_field_bandpass_regression82(self):
#         '''test_plotbandpass: test_multi_field_bandpass_regression82'''
#         plotbandpass('uid___A002_X54d35d_X761.ms.tsys', overlay='antenna', scans='3,6', interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(82),antenna='0,1',debug=False)
  #
# TODO

    # This directory got deleted.
    # if (tstart==0 or tstart==51) and False:
    # ALMA data, casa 3.4 tables: (but scan numbers are not correct: -1)
    #  os.chdir('/lustre/cv/users/thunter/alma/SV_data_TWHya/3.4guide')
    #  51
    #  plotbandpass(caltable='bandpass_all_bpoly_dv04',xaxis='freq',yaxis='both',interactive=False,
    #                  buildpdf=True,figfile=figdir+'regression%02d'%(51),debug=False)
    #

    # This got deleted.
    # if (tstart==0 or tstart==52) and False:
    # ALMA data, FDM+TDM in same caltable
    #  os.chdir('/lustre/cv/users/thunter/alma/SV_data_TWHya/3.4guide')
    # 52   # gives invalid value in double_scalars
    #  plotbandpass(caltable='bandpass_all_b_odd_dv04',caltable2='bandpass_all_bpoly_dv04',xaxis='freq',
    #             yaxis='both',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(52),debug=False)

    # This got deleted.
    #  os.chdir('/lustre/cv/users/thunter/alma/SV_data_TWHya/3.4guide')
    # 53
    #  plotbandpass(caltable='bandpass_all_b_odd_dv04',xaxis='freq',yaxis='phase',chanrange='',showatm=True,
    #                  interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(53),debug=False)

    # This got deleted.
    # if (tstart==0 or tstart==54 or overlayTime or tstart==52 or overlayAntennaTime) and False:
    # 54
    #  os.chdir('/lustre/cv/users/thunter/alma/SV_data_TWHya/3.4guide')
    #  plotbandpass(caltable='bandpass_all_b_odd_dv04',xaxis='freq',yaxis='amp',chanrange='',showatm=True,
    #                  overlay='antenna,time',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(54),debug=False)

    # This got deleted.
    # if (tstart==0 or tstart==55 or overlayTime or overlayAntennaTime) and False:
    #  os.chdir('/lustre/naasc/users/thunter/alma/SV_data_TWHya/3.4guide')
    # 55
    #  plotbandpass(caltable='bandpass_all_b_odd_dv04',xaxis='freq',yaxis='phase',chanrange='',showatm=True,
    #                  overlay='antenna,time',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(55),debug=False)
    #

# class plotbandpass_ALMA_1_pol_test(unittest.TestCase):

#     @classmethod
#     def setUpClass(cls):
#         pass

#     def setUp(self):
#         os.symlink(datapath+'singlePol/uid___A002_X494155_X23a.ms.split.bandpass', os.getcwd() + '/uid___A002_X494155_X23a.ms.split.bandpass')
#         os.symlink(datapath+'singlePol/uid___A002_X494155_X23a.ms.tsys', os.getcwd() + '/uid___A002_X494155_X23a.ms.tsys')

#     def tearDown(self):
#         os.unlink(os.getcwd() + '/uid___A002_X494155_X23a.ms.tsys')
#         os.unlink(os.getcwd() + '/uid___A002_X494155_X23a.ms.split.bandpass')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)

#     # 56
#     @unittest.skip("N/A")
#     def test_ALMA_1_pol_regression56(self):
#         '''test_plotbandpass: test_ALMA_1_pol_regression56'''
#         plotbandpass('uid___A002_X494155_X23a.ms.split.bandpass',xaxis='freq',interactive=False, buildpdf=True,figfile=figdir+'regression%02d'%(56),debug=False)

    #
    # ALMA 4-pol data:  (scan numbers not correct)  This dataset got deleted.
    #  os.chdir('/lustre/naasc/users/thunter/cycle1/4pol')
    # 57
    #  plotbandpass('uid___A002_X60e47a_X8a6.ms.spw0_2_4_6.solintinf_7_8125MHz.bcal.s8_2.tbl',xaxis='freq',
    #                  interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(57),debug=False)
    # 58
    #  plotbandpass('uid___A002_X60e47a_X8a6.ms.spw0_2_4_6.solintinf_7_8125MHz.bcal.s8_2.tbl',xaxis='freq',
    #                  basebands='1,4',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(58),debug=False)


# class plotbandpass_ALMA_multi_regions_test(unittest.TestCase):

#     @classmethod
#     def setUpClass(cls):
#         pass

#     def setUp(self):
#         os.symlink(datapath+'diah/uid___A002_X50bcb3_X3b.ms', os.getcwd() + '/uid___A002_X50bcb3_X3b.ms')
#         os.symlink(datapath+'diah/uid___A002_X50bcb3_X3b.ms.tsys', os.getcwd() + '/uid___A002_X50bcb3_X3b.ms.tsys')

#     def tearDown(self):
#         os.unlink(os.getcwd() + '/uid___A002_X50bcb3_X3b.ms.tsys')
#         os.unlink(os.getcwd() + '/uid___A002_X50bcb3_X3b.ms')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)

#     # ALMA multi-regions per baseband  OUS = uid://A002/X50bb1c/X6  scan numbers=2,7,14,23

#     # 59
#     @unittest.skip("N/A")
#     def test_ALMA_multi_regions_regression59(self):
#         '''test_plotbandpass: test_ALMA_multi_regions_regression59'''
#         plotbandpass('uid___A002_X50bcb3_X3b.ms.tsys',xaxis='freq',showfdm=True,showBasebandNumber=False,
#                   interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(59),debug=False)

#     # 60
#     @unittest.skip("N/A")
#     def test_ALMA_multi_regions_regression60(self):
#         '''test_plotbandpass: test_ALMA_multi_regions_regression60'''
#         plotbandpass('uid___A002_X50bcb3_X3b.ms.tsys',xaxis='freq',showfdm=True,showBasebandNumber=True,
#                   interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(60),debug=False)

# class plotbandpass_EVLA_dual_pol_test(unittest.TestCase):

#     @classmethod
#     def setUpClass(cls):
#         pass

#     def setUp(self):
#         os.symlink(datapath+'evla/10C-186/K_BnA_cont/bandpass_4.1.0.bcal', os.getcwd() + '/bandpass_4.1.0.bcal')
#         os.symlink(datapath+'evla/10C-186/K_BnA_cont/bandpasspcal.bcal', os.getcwd() + '/bandpasspcal.bcal')
#         os.symlink(datapath+'evla/10C-186/K_BnA_cont/K_BnA_cont_multi.ms', os.getcwd() + '/K_BnA_cont_multi.ms')

#     def tearDown(self):
#         os.unlink(os.getcwd() + '/bandpass_4.1.0.bcal')
#         os.unlink(os.getcwd() + '/bandpasspcal.bcal')
#         os.unlink(os.getcwd() + '/K_BnA_cont_multi.ms')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)

#     # EVLA dual-pol data:
#     # 61
#     @unittest.skip("N/A")
#     def test_EVLA_dual_pol_regression61(self):
#         '''test_plotbandpass: test_EVLA_dual_pol_regression61'''
#     # new style table from 4.1.0
#         plotbandpass('bandpass_4.1.0.bcal',poln='',yaxis='amp',overlay='spw',xaxis='freq',interactive=False,
#              buildpdf=True,figfile=figdir+'regression%02d'%(61),debug=False,showBasebandNumber=True)


#     # 62
#     @unittest.skip("N/A")
#     def test_EVLA_dual_pol_regression62(self):
#         '''test_plotbandpass: test_EVLA_dual_pol_regression62'''
#         plotbandpass('bandpass_4.1.0.bcal',poln='',yaxis='amp',overlay='baseband',xaxis='freq',interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(62),debug=False)

#     # 63
#     @unittest.skip("N/A")
#     def test_EVLA_dual_pol_regression63(self):
#         '''test_plotbandpass: test_EVLA_dual_pol_regression63'''
#         plotbandpass('bandpass_4.1.0.bcal',poln='',yaxis='amp',overlay='baseband',xaxis='freq',spw='0,1,14,15',
#                   interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(63),debug=False)

#     # 64
#     @unittest.skip("N/A")
#     def test_EVLA_dual_pol_regression64(self):
#         '''test_plotbandpass: test_EVLA_dual_pol_regression64'''
#         plotbandpass('bandpass_4.1.0.bcal',poln='',yaxis='amp',overlay='spw',xaxis='freq',basebands=12,
#                   showBasebandNumber=True,interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(64),debug=False)

#     # old style 3.3 tables
#     # 65  different antennas have solutions at different times
#     @unittest.skip("N/A")
#     def test_EVLA_dual_pol_regression65(self):
#         '''test_plotbandpass: test_EVLA_dual_pol_regression65'''
#         plotbandpass('bandpasspcal.bcal',poln='',yaxis='amp',overlay='spw',xaxis='freq',interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(65),debug=False)

#     # 66
#     @unittest.skip("N/A")
#     def test_EVLA_dual_pol_regression66(self):
#         '''test_plotbandpass: test_EVLA_dual_pol_regression66'''
#         plotbandpass('bandpasspcal.bcal',poln='',yaxis='amp',overlay='baseband',xaxis='freq',interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(66),debug=False)

#     # 67
#     @unittest.skip("N/A")
#     def test_EVLA_dual_pol_regression67(self):
#         '''test_plotbandpass: test_EVLA_dual_pol_regression67'''
#         plotbandpass('bandpasspcal.bcal',poln='',yaxis='amp',overlay='time',xaxis='freq',interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(67),debug=False)

#     # 68
#     @unittest.skip("N/A")
#     def test_EVLA_dual_pol_regression68(self):
#         '''test_plotbandpass: test_EVLA_dual_pol_regression68'''
#         plotbandpass('bandpasspcal.bcal',poln='',yaxis='both',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(68),debug=False)

#     # 69
#     @unittest.skip("N/A")
#     def test_EVLA_dual_pol_regression69(self):
#         '''test_plotbandpass: test_EVLA_dual_pol_regression69'''
#         plotbandpass('bandpasspcal.bcal',poln='',yaxis='both',showatm=True,interactive=False,buildpdf=True,
#                   figfile=figdir+'regression%02d'%(69),debug=False)

#     # 70
#     @unittest.skip("N/A")
#     def test_EVLA_dual_pol_regression70(self):
#         '''test_plotbandpass: test_EVLA_dual_pol_regression70'''
#         plotbandpass('bandpasspcal.bcal',poln='',yaxis='phase',overlay='antenna',interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(70),debug=False)

#     # 71
#     @unittest.skip("N/A")
#     def test_EVLA_dual_pol_regression71(self):
#         '''test_plotbandpass: test_EVLA_dual_pol_regression71'''
#         plotbandpass('bandpasspcal.bcal',poln='',yaxis='amp',overlay='antenna',chanrange='0~30',
#              xaxis='freq',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(71),
#              debug=False, chanrangeSetXrange=True)

#     # 72
#     @unittest.skip("N/A")
#     def test_EVLA_dual_pol_regression72(self):
#         '''test_plotbandpass: test_EVLA_dual_pol_regression72'''
#         plotbandpass('bandpasspcal.bcal',poln='',yaxis='both',showatm=True,interactive=False,buildpdf=True,
#                   figfile=figdir+'regression%02d'%(72),debug=False)

#     # 73
#     @unittest.skip("N/A")
#     def test_EVLA_dual_pol_regression73(self):
#         '''test_plotbandpass: test_EVLA_dual_pol_regression73'''
#         plotbandpass('bandpasspcal.bcal',poln='LL',yaxis='both',interactive=False,buildpdf=True,
#                   figfile=figdir+'regression%02d'%(73),debug=False)

# class plotbandpass_EVLA_single_pol_test(unittest.TestCase):

#     @classmethod
#     def setUpClass(cls):
#         pass

#     def setUp(self):
#         os.symlink(datapath+'evla/AB1346/g19.36/bandpass.bcal', os.getcwd() + '/bandpass.bcal')
#         os.symlink(datapath+'evla/AB1346/g19.36/g19.36_I_3sD_multi.ms', os.getcwd() + '/g19.36_I_3sD_multi.ms')


#     def tearDown(self):
#         os.unlink(os.getcwd() + '/bandpass.bcal')
#         os.unlink(os.getcwd() + '/g19.36_I_3sD_multi.ms')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)

#     # EVLA single-pol data:
#     # new style table from 4.1.0
#     # old style 3.3 tables
#     # 74
#     @unittest.skip("N/A")
#     def test_EVLA_single_pol_regression74(self):
#         '''test_plotbandpass: test_EVLA_single_pol_regression74'''
#         plotbandpass('bandpass.bcal',yaxis='amp',xaxis='freq',overlay='spw',interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(74),debug=False)


#     # 75
#     @unittest.skip("N/A")
#     def test_EVLA_single_pol_regression75(self):
#         '''test_plotbandpass: test_EVLA_single_pol_regression75'''
#         plotbandpass('bandpass.bcal',yaxis='phase',xaxis='freq',overlay='baseband',interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(75),debug=False)


#     # 76
#     @unittest.skip("N/A")
#     def test_EVLA_single_pol_regression76(self):
#         '''test_plotbandpass: test_EVLA_single_pol_regression76'''
#         plotbandpass('bandpass.bcal',caltable2='bandpass_bpoly.bcal',yaxis='both',xaxis='freq',
#                   interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(76),debug=False)

#     # 77
#     @unittest.skip("N/A")
#     def test_EVLA_single_pol_regression77(self):
#         '''test_plotbandpass: test_EVLA_single_pol_regression77'''
#         plotbandpass('bandpass.bcal',caltable2='bandpass_bpoly.bcal',yaxis='both',xaxis='freq',
#                   showatm=True,interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(77),debug=False)

#     # 114
#     @unittest.skip("N/A")
#     def test_EVLA_single_pol_regression114(self):
#         '''test_plotbandpass: test_EVLA_single_pol_regression114'''
#         plotbandpass('bandpass.bcal',overlay='baseband',yaxis='both',xaxis='freq',
#              interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(114),debug=False)

# class plotbandpass_SMA_ngc6334_SM2_filler_test(unittest.TestCase):

#     @classmethod
#     def setUpClass(cls):
#         pass

#     def setUp(self):
#         os.symlink(datapath+'sma/ngc6334_SM2_filler/sm2_rx0.usb.if2.tsys.ms', os.getcwd() + '/sm2_rx0.usb.if2.tsys.ms')
#         os.symlink(datapath+'sma/ngc6334_SM2_filler/sm2_rx0.usb.if2.tsys.ms.bandpass.bcal', os.getcwd() + '/sm2_rx0.usb.if2.tsys.ms.bandpass.bcal')


#     def tearDown(self):
#         os.unlink(os.getcwd() + '/sm2_rx0.usb.if2.tsys.ms')
#         os.unlink(os.getcwd() + '/sm2_rx0.usb.if2.tsys.ms.bandpass.bcal')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)

#     # 78
#     @unittest.skip("N/A")
#     def test_SMA_ngc6334_SM2_filler_regression78(self):
#         '''test_plotbandpass: test_SMA_ngc6334_SM2_filler_regression78'''
#     # SMA data
#         plotbandpass('sm2_rx0.usb.if2.tsys.ms.bandpass.bcal',interactive=False,
#                   buildpdf=True,figfile=figdir+'regression%02d'%(78),debug=False)

#     # 79
#     @unittest.skip("N/A")
#     def test_SMA_ngc6334_SM2_filler_regression79(self):
#         '''test_plotbandpass: test_SMA_ngc6334_SM2_filler_regression79'''
#         plotbandpass('sm2_rx0.usb.if2.tsys.ms.bandpass.bcal',overlay='baseband',
#                   xaxis='freq',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(79),debug=False)

#     # 80
#     @unittest.skip("N/A")
#     def test_SMA_ngc6334_SM2_filler_regression80(self):
#         '''test_plotbandpass: test_SMA_ngc6334_SM2_filler_regression80'''
#         plotbandpass('sm2_rx0.usb.if2.tsys.ms.bandpass.bcal',overlay='baseband',figfileSequential=True,
#                   xaxis='freq',interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(80),debug=False)

# class plotbandpass_3_test(unittest.TestCase):

#     @classmethod
#     def setUpClass(cls):
#         pass

#     def setUp(self):
#         os.symlink(datapath+'uid___A002_X61d35c_X4f.ms', os.getcwd() + '/uid___A002_X61d35c_X4f.ms')
#         os.symlink(datapath+'uid___A002_X61d35c_X4f.ms.tsys', os.getcwd() + '/uid___A002_X61d35c_X4f.ms.tsys')


#     def tearDown(self):
#         os.unlink(os.getcwd() + '/uid___A002_X61d35c_X4f.ms')
#         os.unlink(os.getcwd() + '/uid___A002_X61d35c_X4f.ms.tsys')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)

#     # 83
#     @unittest.skip("N/A")
#     def test_uid___A002_X61d35c_X4f_regression83(self):
#         '''test_plotbandpass: test_uid___A002_X61d35c_X4f_regression83'''
#         plotbandpass('uid___A002_X61d35c_X4f.ms.tsys',chanrange='5~122',showBasebandNumber=True,
#                     groupByBaseband=True,xaxis='freq',interactive=False,buildpdf=True,
#                     figfile=figdir+'regression%02d'%(83),debug=False, chanrangeSetXrange=True)

#     # 84
#     @unittest.skip("N/A")
#     def test_uid___A002_X61d35c_X4f_regression84(self):
#         '''test_plotbandpass: test_uid___A002_X61d35c_X4f_regression84'''
#         plotbandpass('uid___A002_X61d35c_X4f.ms.tsys',showBasebandNumber=True,
#         #      chanrange='5~122',
#                chanrange='92.1875%',
#                xaxis='freq',interactive=False,buildpdf=True,
#                figfile=figdir+'regression%02d'%(84), antenna=0,debug=False, chanrangeSetXrange=True)

#     # 85
#     @unittest.skip("N/A")
#     def test_uid___A002_X61d35c_X4f_regression85(self):
#         '''test_plotbandpass: test_uid___A002_X61d35c_X4f_regression85'''
#         plotbandpass('uid___A002_X61d35c_X4f.ms.tsys',chanrange='5~122',showBasebandNumber=True,
#                xaxis='freq',interactive=False,buildpdf=True, chanrangeSetXrange=True,
#                figfile=figdir+'regression%02d'%(85),basebands='1,3',antenna='0,1',debug=False)

#     # 86
#     @unittest.skip("N/A")
#     def test_uid___A002_X61d35c_X4f_regression86(self):
#         '''test_plotbandpass: test_uid___A002_X61d35c_X4f_regression86'''
#         plotbandpass('uid___A002_X61d35c_X4f.ms.tsys',chanrange='',showBasebandNumber=True,
#                     xaxis='freq',interactive=False,buildpdf=True,
#                     figfile=figdir+'regression%02d'%(86),overlay='spw',antenna='0,1',debug=False)

#     # 87
#     @unittest.skip("N/A")
#     def test_uid___A002_X61d35c_X4f_regression87(self):
#         '''test_plotbandpass: test_uid___A002_X61d35c_X4f_regression87'''
#         plotbandpass('uid___A002_X61d35c_X4f.ms.tsys',chanrange='',showBasebandNumber=True,
#                     xaxis='freq',interactive=False,buildpdf=True,
#                     figfile=figdir+'regression%02d'%(87),overlay='baseband',antenna='0,1',debug=False)

#     # 88
#     @unittest.skip("N/A")
#     def test_uid___A002_X61d35c_X4f_regression88(self):
#         '''test_plotbandpass: test_uid___A002_X61d35c_X4f_regression88'''
#         plotbandpass('uid___A002_X61d35c_X4f.ms.tsys',chanrange='5~122',showBasebandNumber=True,
#                xaxis='freq',interactive=False,buildpdf=True, chanrangeSetXrange=True,
#                figfile=figdir+'regression%02d'%(88),overlay='time',showatm=True,showfdm=True,debug=False)

#     # 89
#     @unittest.skip("N/A")
#     def test_uid___A002_X61d35c_X4f_regression89(self):
#         '''test_plotbandpass: test_uid___A002_X61d35c_X4f_regression89'''
#         plotbandpass('uid___A002_X61d35c_X4f.ms.tsys',chanrange='5~122',showBasebandNumber=True,
#                xaxis='freq',interactive=False,buildpdf=True,showfdm=True, chanrangeSetXrange=True,
#                figfile=figdir+'regression%02d'%(89),overlay='antenna',antenna='!DV17',showatm=True,debug=False)

    #TODO
    # ALMA multi-regions per baseband  OUS = uid://A002/X50bb1c/X6  scan numbers=2,7,14,23
    # single antenna with subplot=11
    #os.chdir(mydir+'/diah')
    # 81
    #myfunction('uid___A002_X50bcb3_X3b.ms.tsys',xaxis='freq',showfdm=True,showBasebandNumber=False,
    #         interactive=False,buildpdf=False,figfile=figdir+'regression%02d'%(81),
    #         subplot=11,antenna=0,debug=False)
    #au.buildPdfFromPngs(pnglist=figdir+'regression%02d*.png'%(81), pdfname=figdir+'regression%02d.pdf'%(81))



class plotbandpass_CAS_6147_test(unittest.TestCase):
    def setUp(self):
        os.symlink(datapath+'uid___A002_X71a45c_X1d24.ms.split', os.getcwd() + '/uid___A002_X71a45c_X1d24.ms.split')
        os.symlink(datapath+'uid___A002_X71a45c_X1d24.ms.split.bandpass', os.getcwd() + '/uid___A002_X71a45c_X1d24.ms.split.bandpass')

    def tearDown(self):
        os.unlink(os.getcwd() + '/uid___A002_X71a45c_X1d24.ms.split')
        os.unlink(os.getcwd() + '/uid___A002_X71a45c_X1d24.ms.split.bandpass')
        if delete_artifacts:
            artifacts = os.listdir(figdir)
            for artifact in artifacts:
                if artifact.endswith(".pdf") or artifact.endswith(".png"):
                    os.remove(artifact)

    # 90   CAS-6147: cannot handle this table
    def test_CAS_6147_regression90(self):
        '''test_plotbandpass: test_CAS_6147_regression90'''
        plotbandpass('uid___A002_X71a45c_X1d24.ms.split.bandpass',interactive=False,buildpdf=True,
               figfile=figdir+'regression%02d'%(90),debug=False)


class plotbandpass_CAS_6111_test(unittest.TestCase):
    def setUp(self):
        os.symlink(datapath+'uid___A002_X5e971a_X124.ms', os.getcwd() + '/uid___A002_X5e971a_X124.ms')
        os.symlink(datapath+'uid___A002_X5e971a_X124.ms.hifa_tsyscal.s5_2.tsyscal.tbl', os.getcwd() + '/uid___A002_X5e971a_X124.ms.hifa_tsyscal.s5_2.tsyscal.tbl')

    def tearDown(self):
        os.unlink(os.getcwd() + '/uid___A002_X5e971a_X124.ms')
        os.unlink(os.getcwd() + '/uid___A002_X5e971a_X124.ms.hifa_tsyscal.s5_2.tsyscal.tbl')
        if delete_artifacts:
            artifacts = os.listdir(figdir)
            for artifact in artifacts:
                if artifact.endswith(".pdf") or artifact.endswith(".png"):
                    os.remove(artifact)

    # 91   CAS-6111: CM01 completely flagged causes no plot produced
    def test_CAS_6111_regression91(self):
        '''test_plotbandpass: test_CAS_6111_regression91'''
        plotbandpass('uid___A002_X5e971a_X124.ms.hifa_tsyscal.s5_2.tsyscal.tbl',overlay='antenna,time',interactive=False,
               buildpdf=True, figfile=figdir+'regression%02d'%(91),debug=False)


class plotbandpass_CAS_6356_test(unittest.TestCase):
    def setUp(self):
        os.symlink(datapath+'uid___A002_X7b13df_X68f.ms', os.getcwd() + '/uid___A002_X7b13df_X68f.ms')
        os.symlink(datapath+'uid___A002_X7b13df_X68f.ms.tsys', os.getcwd() + '/uid___A002_X7b13df_X68f.ms.tsys')

    def tearDown(self):
        os.unlink(os.getcwd() + '/uid___A002_X7b13df_X68f.ms')
        os.unlink(os.getcwd() + '/uid___A002_X7b13df_X68f.ms.tsys')
        if delete_artifacts:
            artifacts = os.listdir(figdir)
            for artifact in artifacts:
                if artifact.endswith(".pdf") or artifact.endswith(".png"):
                    os.remove(artifact)

    # 92  CAS-6356: error in the legend colors
    def test_CAS_6356_regression92(self):
        '''test_plotbandpass: test_CAS_6356_regression92'''
        plotbandpass('uid___A002_X7b13df_X68f.ms.tsys',overlay='time',interactive=False,buildpdf=True,
               figfile=figdir+'regression%02d'%(92),debug=False)

class plotbandpass_CAS_7368_test(unittest.TestCase):
    def setUp(self):
        os.symlink(datapath+'finalBPcal.b', os.getcwd() + '/finalBPcal.b')

    def tearDown(self):
        os.unlink(os.getcwd() + '/finalBPcal.b')
        if delete_artifacts:
            artifacts = os.listdir(figdir)
            for artifact in artifacts:
                if artifact.endswith(".pdf") or artifact.endswith(".png"):
                    os.remove(artifact)

    # 107   CAS-7368  VLA pipeline dataset (amp)
    def test_CAS_7368_regression107(self):
        '''test_plotbandpass: test_CAS_7368_regression107'''
        plotbandpass(subplot=11, antenna=0, overlay='baseband',
               caltable='finalBPcal.b', figfile=figdir+'regression107', yaxis='amp',
               xaxis='freq', interactive=False, buildpdf=True)

    # 108   CAS-7368  VLA pipeline dataset (amp with showatm)
    def test_CAS_7368_regression108(self):
        '''test_plotbandpass: test_CAS_7368_regression108'''
        plotbandpass(subplot=11, antenna=0, overlay='baseband', showatm=True,
               caltable='finalBPcal.b', figfile=figdir+'regression108', yaxis='amp',
               xaxis='freq', interactive=False, buildpdf=True, debug=False)

    # 109   CAS-7368  VLA pipeline dataset (phase)
    def test_CAS_7368_regression109(self):
        '''test_plotbandpass: test_CAS_7368_regression109'''
        plotbandpass(subplot=11, antenna=0, overlay='baseband',
               caltable='finalBPcal.b', figfile=figdir+'regression109', yaxis='phase',
               xaxis='freq', interactive=False, buildpdf=True)

class plotbandpass_CAS_7965_test(unittest.TestCase):
    def setUp(self):
        os.symlink(datapath+'uid___A002_X99c183_X25b6.ms.split.bandpass', os.getcwd() + '/uid___A002_X99c183_X25b6.ms.split.bandpass')

    def tearDown(self):
        os.unlink(os.getcwd() + '/uid___A002_X99c183_X25b6.ms.split.bandpass')
        if delete_artifacts:
            artifacts = os.listdir(figdir)
            for artifact in artifacts:
                if artifact.endswith(".pdf") or artifact.endswith(".png"):
                    os.remove(artifact)

    # 115 CAS-7965
    def test_CAS_7965_regression115(self):
        '''test_plotbandpass: test_CAS_7965_regression115'''
        plotbandpass('uid___A002_X99c183_X25b6.ms.split.bandpass',subplot=11,overlay='baseband',xaxis='freq',
               chanrange='90%', interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(115))


class plotbandpass_CAS_7715_test(unittest.TestCase):
    def setUp(self):
        os.symlink(datapath+'uid___A002_Xa1f062_X37e3.ms', os.getcwd() + '/uid___A002_Xa1f062_X37e3.ms')
        os.symlink(datapath+'uid___A002_Xa1f062_X37e3.ms.tsys', os.getcwd() + '/uid___A002_Xa1f062_X37e3.ms.tsys')
        os.symlink(datapath+'uid___A002_Xa2ce2e_X54b.ms', os.getcwd() + '/uid___A002_Xa2ce2e_X54b.ms')
        os.symlink(datapath+'uid___A002_Xa2ce2e_X54b.ms.tsys', os.getcwd() + '/uid___A002_Xa2ce2e_X54b.ms.tsys')

    def tearDown(self):
        os.unlink(os.getcwd() + '/uid___A002_Xa1f062_X37e3.ms')
        os.unlink(os.getcwd() + '/uid___A002_Xa1f062_X37e3.ms.tsys')
        os.unlink(os.getcwd() + '/uid___A002_Xa2ce2e_X54b.ms')
        os.unlink(os.getcwd() + '/uid___A002_Xa2ce2e_X54b.ms.tsys')
        if delete_artifacts:
            artifacts = os.listdir(figdir)
            for artifact in artifacts:
                if artifact.endswith(".pdf") or artifact.endswith(".png"):
                    os.remove(artifact)

    # 116 CAS-7715  64-channel Band 9 Tsys showing offset in image sideband
    def test_CAS_7715_regression116(self):
        '''test_plotbandpass: CAS-7715, 64-channel Band 9 Tsys showing offset in image sideband'''
        plotbandpass('uid___A002_Xa1f062_X37e3.ms.tsys',subplot=11,overlay='time',xaxis='freq',
               showatm=True, showimage=True,
               spw='30,32', interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(116))

    # 117 CAS-7715  128-channel Band 9 Tsys showing offset in image sideband
    def test_CAS_7715_regression117(self):
        '''test_plotbandpass: CAS-7715, 128-channel Band 9 Tsys showing offset in image sideband'''
        plotbandpass('uid___A002_Xa2ce2e_X54b.ms.tsys',subplot=11,overlay='time',xaxis='freq',
               showatm=True, showimage=True,
               spw='9', interactive=False,buildpdf=True,figfile=figdir+'regression%02d'%(117))


class plotbandpass_CAS_8261_test(unittest.TestCase):
    def setUp(self):
        os.symlink(datapath+'uid___A002_Xaf2188_X213a.ms', os.getcwd() + '/uid___A002_Xaf2188_X213a.ms')
        os.symlink(datapath+'uid___A002_Xaf2188_X213a.ms.hifa_tsyscal.s6_3.tsyscal.tbl', os.getcwd() + '/uid___A002_Xaf2188_X213a.ms.hifa_tsyscal.s6_3.tsyscal.tbl')

    def tearDown(self):
        os.unlink(os.getcwd() + '/uid___A002_Xaf2188_X213a.ms')
        os.unlink(os.getcwd() + '/uid___A002_Xaf2188_X213a.ms.hifa_tsyscal.s6_3.tsyscal.tbl')
        if delete_artifacts:
            artifacts = os.listdir(figdir)
            for artifact in artifacts:
                if artifact.endswith(".pdf") or artifact.endswith(".png"):
                    os.remove(artifact)

    # 118 CAS-8261
    def test_CAS_8261_regression118(self):
        '''test_plotbandpass: test_CAS_8261_regression118'''
        plotbandpass('uid___A002_Xaf2188_X213a.ms.hifa_tsyscal.s6_3.tsyscal.tbl',subplot=11,chanrange='90%',
               antenna='42,43,24,25,26,27,20,21,22,23,28,29,40,41,1,0,3,2,5,4,7,6,9,8,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32',
               yaxis='tsys', overlay='time', showatm=True, spw='19,21,17,23', xaxis='freq', showfdm=True,
               interactive=False, buildpdf=True,figfile=figdir+'regression%02d'%(118))

class plotbandpass_CAS_8489_test(unittest.TestCase):
    def setUp(self):
        os.symlink(datapath+'uid___A002_X85c183_X60b.skycal.spw23.tbl', os.getcwd() + '/uid___A002_X85c183_X60b.skycal.spw23.tbl')
        os.symlink(datapath+'uid___A002_Xb0dfe8_Xcc8.orion_sio.skycal.tbl', os.getcwd() + '/uid___A002_Xb0dfe8_Xcc8.orion_sio.skycal.tbl')

    def tearDown(self):
        os.unlink(os.getcwd() + '/uid___A002_X85c183_X60b.skycal.spw23.tbl')
        os.unlink(os.getcwd() + '/uid___A002_Xb0dfe8_Xcc8.orion_sio.skycal.tbl')
        if delete_artifacts:
            artifacts = os.listdir(figdir)
            for artifact in artifacts:
                if artifact.endswith(".pdf") or artifact.endswith(".png"):
                    os.remove(artifact)

    # 119 CAS-8489, selection by timerange relative to scan
    def test_CAS_8489_regression119(self):
        '''test_plotbandpass: CAS-8489, selection by timerange relative to scan'''

        plotbandpass('uid___A002_X85c183_X60b.skycal.spw23.tbl',scans='7',spw=23,timeranges='1,2',antenna=0,
               interactive=False, buildpdf=True,figfile=figdir+'regression%02d'%(119))

    # 120 CAS-8489, overlay baseband x-axis range
    def test_CAS_8489_regression120(self):
        '''test_plotbandpass: CAS-8489, overlay baseband x-axis range'''

        plotbandpass('uid___A002_Xb0dfe8_Xcc8.orion_sio.skycal.tbl',overlay='baseband',antenna=0,timeranges='0',
               xaxis='freq', interactive=False, buildpdf=True,figfile=figdir+'regression%02d'%(120))


    # 121 CAS-8489 final, overlay baseband y-axis range
    def test_CAS_8489_regression121(self):
        '''test_plotbandpass: CAS-8489, overlay baseband y-axis range'''
        plotbandpass('uid___A002_Xb0dfe8_Xcc8.orion_sio.skycal.tbl',overlay='baseband',field='3',antenna=0,
               timeranges='0',scans='9',subplot=11,spw='23,25,27,29',showatm=True,yaxis='amp',
               xaxis='freq', interactive=False, buildpdf=True,figfile=figdir+'regression%02d'%(121))

class plotbandpass_CAS_8655_test(unittest.TestCase):
    def setUp(self):
        os.symlink(datapath+'uid___A002_X960614_X1379.ms.hifa_tsyscal.s6_3.tsyscal.tbl', os.getcwd() + '/uid___A002_X960614_X1379.ms.hifa_tsyscal.s6_3.tsyscal.tbl')

    def tearDown(self):
        os.unlink(os.getcwd() + '/uid___A002_X960614_X1379.ms.hifa_tsyscal.s6_3.tsyscal.tbl')
        if delete_artifacts:
            artifacts = os.listdir(figdir)
            for artifact in artifacts:
                if artifact.endswith(".pdf") or artifact.endswith(".png"):
                    os.remove(artifact)

    # 122 CAS-8655
    def test_CAS_8655_regression122(self):
        '''test_plotbandpass: test_CAS_8655_regression122'''
        plotbandpass('uid___A002_X960614_X1379.ms.hifa_tsyscal.s6_3.tsyscal.tbl/',overlay='antenna,time',
               subplot=11,spw='9',showatm=True,yaxis='amp',chanrange='90%',
               xaxis='freq', interactive=False, buildpdf=True,figfile=figdir+'regression%02d'%(122))
        #vis='/lustre/naasc/sciops/comm/rindebet/pipeline/root/2013.1.01194.S_2016_05_23T02_55_30.859/SOUS_uid___A001_X11f_X4a/GOUS_uid___A001_X11f_X4b/MOUS_uid___A001_X11f_X4c/working/uid___A002_X960614_X1379.ms',

    #if (tstart==122 or tstart==0 or overlaySpw):
    #    # CAS-8489 final, overlay spw y-axis range
    #    os.chdir(mydir+'/regression/CAS-8489')
    #    plotbandpass('uid___A002_Xb0dfe8_Xcc8.orion_sio.skycal.tbl',overlay='spw',field='3',antenna=0,
    #               timeranges='0',scans='9',subplot=11,spw='23,25,27,29',showatm=True,yaxis='amp',
    #               xaxis='freq', interactive=False, buildpdf=True,figfile=figdir+'regression%02d'%(122))


class plotbandpass_CAS_9474_test(unittest.TestCase):
    def setUp(self):
        os.symlink(datapath+'16B-402.sb32888048.eb33072402.57733.81140140047.ms.finalBPcal.b', os.getcwd() + '/16B-402.sb32888048.eb33072402.57733.81140140047.ms.finalBPcal.b')
        os.symlink(datapath+'16B-402.sb32888048.eb33072402.57733.81140140047.ms.finalBPcal_sm.b', os.getcwd() + '/16B-402.sb32888048.eb33072402.57733.81140140047.ms.finalBPcal_sm.b')

    def tearDown(self):
        os.unlink(os.getcwd() + '/16B-402.sb32888048.eb33072402.57733.81140140047.ms.finalBPcal.b')
        os.unlink(os.getcwd() + '/16B-402.sb32888048.eb33072402.57733.81140140047.ms.finalBPcal_sm.b')
        if delete_artifacts:
            artifacts = os.listdir(figdir)
            for artifact in artifacts:
                if artifact.endswith(".pdf") or artifact.endswith(".png"):
                    os.remove(artifact)

    # 123 CAS-9474
    def test_CAS_9474_regression123(self):
        '''test_plotbandpass: test_CAS_9474_regression123'''
        plotbandpass('16B-402.sb32888048.eb33072402.57733.81140140047.ms.finalBPcal_sm.b',spw='19',
               caltable2='16B-402.sb32888048.eb33072402.57733.81140140047.ms.finalBPcal.b',
               xaxis='freq', interactive=False, buildpdf=True,figfile=figdir+'regression%02d'%(123))

class plotbandpass_SCOPS_4877_test(unittest.TestCase):
    def setUp(self):
        os.symlink(datapath+'uid___A002_Xbf792a_X26ec.ms', os.getcwd() + '/uid___A002_Xbf792a_X26ec.ms')
        os.symlink(datapath+'uid___A002_Xbf792a_X26ec.ms.tsys', os.getcwd() + '/uid___A002_Xbf792a_X26ec.ms.tsys')

    def tearDown(self):
        os.unlink(os.getcwd() + '/uid___A002_Xbf792a_X26ec.ms')
        os.unlink(os.getcwd() + '/uid___A002_Xbf792a_X26ec.ms.tsys')
        if delete_artifacts:
            artifacts = os.listdir(figdir)
            for artifact in artifacts:
                if artifact.endswith(".pdf") or artifact.endswith(".png"):
                    os.remove(artifact)

    # 124 SCOPS-4877  first antenna flagged causes LO1 undefined
    def test_SCOPS_4877_regression124(self):
        '''test_plotbandpass: test_SCOPS_4877_regression124'''
        plotbandpass('uid___A002_Xbf792a_X26ec.ms.tsys',overlay='time',showimage=True,showatm=True,showBasebandNumber=True,
               xaxis='freq', interactive=False, buildpdf=True,figfile=figdir+'regression%02d'%(124))

class plotbandpass_CAS_14119_test(unittest.TestCase):
    def setUp(self):
        os.symlink(datapath+'uid___A002_Xb0dfe8_Xcc8.orion_sio.skycal.tbl', os.getcwd() + '/uid___A002_Xb0dfe8_Xcc8.orion_sio.skycal.tbl')
        os.symlink(datapath+'uid___A002_X5e971a_X124.ms.hifa_tsyscal.s5_2.tsyscal.tbl', os.getcwd() + '/uid___A002_X5e971a_X124.ms.hifa_tsyscal.s5_2.tsyscal.tbl')
        os.symlink(datapath+'uid___A002_X5e971a_X124.ms', os.getcwd() + '/uid___A002_X5e971a_X124.ms')

    def tearDown(self):
        os.unlink(os.getcwd() + '/uid___A002_Xb0dfe8_Xcc8.orion_sio.skycal.tbl')
        os.unlink(os.getcwd() + '/uid___A002_X5e971a_X124.ms.hifa_tsyscal.s5_2.tsyscal.tbl')
        os.unlink(os.getcwd() + '/uid___A002_X5e971a_X124.ms')
        if delete_artifacts:
            artifacts = os.listdir(figdir)
            for artifact in artifacts:
                if artifact.endswith(".pdf") or artifact.endswith(".png"):
                    os.remove(artifact)

    # 125 CAS-14119 overlay time colorization
    def test_CAS_14119_regression125(self):
        '''test_plotbandpass: CAS-14119, overlay time colors'''
        # Select two times with same interval as colors list length; should use different color for each time
        plotbandpass('uid___A002_Xb0dfe8_Xcc8.orion_sio.skycal.tbl', overlay='time', field='3', antenna=0,
               timeranges='0,17', spw='23', yaxis='amp', xaxis='freq',
               interactive=False, figfile=figdir+'regression125')

    # 126 (bug found in duplicate ticket 14201) overlay time colorization with showfdm
    def test_CAS_14119_regression126(self):
        '''test_plotbandpass: CAS-14119, overlay time colors with showfdm'''
        # Ensure that overlay plot completes
        plotbandpass(caltable='uid___A002_X5e971a_X124.ms.hifa_tsyscal.s5_2.tsyscal.tbl', overlay='antenna,time',
            showfdm=True, showatm=True, vis='uid___A002_X5e971a_X124.ms', spw='8', subplot=11, figfile=figdir+'regression126',
            chanrange='90%', xaxis='freq', yaxis='tsys', interactive=False, showBasebandNumber=True)

# class plotbandpass_tsysFlagged_test(unittest.TestCase):
#     def setUp(self):
#         os.symlink(datapath+'tsysFlagged/uid___A002_Xa018c4_X2537.ms', os.getcwd() + '/uid___A002_Xa018c4_X2537.ms')
#         os.symlink(datapath+'tsysFlagged/uid___A002_Xa018c4_X2537.ms.tsys', os.getcwd() + '/uid___A002_Xa018c4_X2537.ms.tsys')

#     def tearDown(self):
#         os.unlink(os.getcwd() + '/uid___A002_Xa018c4_X2537.ms')
#         os.unlink(os.getcwd() + '/uid___A002_Xa018c4_X2537.ms.tsys')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)

#     # 110   4-polarization dataset, where all Tsys timeranges flagged on one spw for DV21
#     @unittest.skip("N/A")
#     def test_tsysFlagged_regression107(self):
#         '''test_plotbandpass: test_tsysFlagged_regression107'''
#         plotbandpass(subplot=22, overlay='time',showfdm=True, showatm=True,
#                caltable='uid___A002_Xa018c4_X2537.ms.tsys', figfile=figdir+'regression110',
#                xaxis='freq', interactive=False, buildpdf=True, showBasebandNumber=True)

# class plotbandpass_mislabeling_test(unittest.TestCase):
#     def setUp(self):
#         os.symlink(datapath+'mislabeling/uid___A002_Xa6a120_X1d3d.ms', os.getcwd() + '/uid___A002_Xa6a120_X1d3d.ms')
#         os.symlink(datapath+'mislabeling/uid___A002_Xa6a120_X1d3d.ms.hifa_tsyscal.s6_1.tsyscal.tbl', os.getcwd() + '/uid___A002_Xa6a120_X1d3d.ms.hifa_tsyscal.s6_1.tsyscal.tbl')

#     def tearDown(self):
#         os.unlink(os.getcwd() + '/uid___A002_Xa6a120_X1d3d.ms')
#         os.unlink(os.getcwd() + '/uid___A002_Xa6a120_X1d3d.ms.hifa_tsyscal.s6_1.tsyscal.tbl')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)

#     @unittest.skip("N/A")
#     def test_mislabeling_regression111(self):
#         '''test_plotbandpass: test_mislabeling_regression111'''
#     # 111   lots of flags
#         plotbandpass(subplot=11, overlay='time,antenna',showfdm=True, showatm=True,
#                caltable='uid___A002_Xa6a120_X1d3d.ms.hifa_tsyscal.s6_1.tsyscal.tbl',
#                figfile=figdir+'regression111',
#                xaxis='freq', interactive=False, buildpdf=True, showBasebandNumber=True)

# class plotbandpass_badAntennaFilename_test(unittest.TestCase):
#     def setUp(self):
#         os.symlink(datapath+'badAntennaFilename/uid___A002_Xabe765_X5bd.ms', os.getcwd() + '/uid___A002_Xabe765_X5bd.ms')
#         os.symlink(datapath+'badAntennaFilename/uid___A002_Xabe765_X5bd.ms.bcal', os.getcwd() + '/uid___A002_Xabe765_X5bd.ms.bcal')

#     def tearDown(self):
#         os.unlink(os.getcwd() + '/uid___A002_Xabe765_X5bd.ms')
#         os.unlink(os.getcwd() + '/uid___A002_Xabe765_X5bd.ms.bcal')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)

#     @unittest.skip("N/A")
#     def test_badAntennaFilename_regression112(self):
#         '''test_plotbandpass: test_badAntennaFilename_regression112'''
#         plotbandpass(subplot=11, antenna='3,4', overlay='baseband', caltable='uid___A002_Xabe765_X5bd.ms.bcal',
#                figfile=figdir+'regression112', buildpdf=True,
#                showatm=True, yaxis='amp', xaxis='freq', interactive=False)

# class plotbandpass_badSpwFilename_test(unittest.TestCase):
#     def setUp(self):
#         os.symlink(datapath+'badSpwFilename/uid___A002_Xa2ce2e_X7bd.ms', os.getcwd() + '/uid___A002_Xa2ce2e_X7bd.ms')
#         os.symlink(datapath+'badSpwFilename/uid___A002_Xa2ce2e_X7bd.ms.tsys', os.getcwd() + '/uid___A002_Xa2ce2e_X7bd.ms.tsys')

#     def tearDown(self):
#         os.unlink(os.getcwd() + '/uid___A002_Xa2ce2e_X7bd.ms')
#         os.unlink(os.getcwd() + '/uid___A002_Xa2ce2e_X7bd.ms.tsys')
#         if delete_artifacts:
#             artifacts = os.listdir(figdir)
#             for artifact in artifacts:
#                 if artifact.endswith(".pdf") or artifact.endswith(".png"):
#                     os.remove(artifact)

#     @unittest.skip("N/A")
#     def test_badSpwFilename_regression113(self):
#         '''test_plotbandpass: test_badSpwFilename_regression113'''
#         plotbandpass(subplot=11, antenna='DA58', overlay='time', caltable='uid___A002_Xa2ce2e_X7bd.ms.tsys',
#                figfile=figdir+'regression113', buildpdf=True,
#                showatm=True, yaxis='amp', xaxis='freq', interactive=False)


class plotbandpass_private_test(unittest.TestCase):
    def test_sloppyUnique(self):
        """test sloppyUnique"""
        import casatasks.private.task_plotbandpass as _plotbandpass

        time_list = [1,  30,  59,  88, 117, 146, 175]
        time_threshold = 30  # sec
        unique_time_list = list(_plotbandpass.sloppyUnique(time_list, 30))
        self.assertEqual(unique_time_list, [1, 59, 117, 175])


if __name__ == '__main__':
    unittest.main()
