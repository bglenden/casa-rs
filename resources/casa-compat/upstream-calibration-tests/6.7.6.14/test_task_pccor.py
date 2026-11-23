import shutil
import unittest
import numpy as np
import os

from casatasks import pccor, fringefit
from casatools import table, ctsys

# Useful variables that define default (def_), alternative (alt_) and bad (bad_) values for use in the tests
def_ms = 'BM303W_no_FD_spw_0.ms'
no_phase_cal_ms = 'no_phase_cal_tiny.ms'
contiguous_ms = 'contiguous_split.ms'
non_contiguous_ms = 'non_contiguous_split.ms'

alt_ff_caltable = 'alt_fringe_fit.mpc'

ref_pccor_caltable = 'missing_FD_0.pccor'
def_pccor_caltable = 'pccor_test.pccor'

def_scan = '177'
alt_scan = '188'
bad_scan = '2000'

def_range = '2011/09/16/21:36:00~21:37:00'
alt_range = '22:36:00~22:37:00'
bad_timerange = '2015/09/16/21:36:00~21:37:00'

def_refant = 'LA'
alt_ant = 'PT'
bad_ant = 'FD'

bad_spw = 0
alt_spw = 1

missing_param = 'none'

test_data_files = [def_ms, no_phase_cal_ms, alt_ff_caltable, ref_pccor_caltable]


def run_pccor(vis=def_ms, refant=def_refant, scan=def_scan, timerange=def_range,
              ff_table=missing_param, antenna=missing_param, spw=missing_param):

    ret_dict = pccor(
        vis,
        def_pccor_caltable,
        refant,
        timerange=timerange,
        scan=scan,
        ff_table=ff_table,
        antenna=antenna,
        spw=spw
    )
    return ret_dict


def extract_ff_solution(caltable, antenna, spw):
    ff_tbl = table()
    ff_tbl.open(caltable+'/ANTENNA')
    ant_names = ff_tbl.getcol('NAME')
    ff_tbl.close()
    ant_ids = np.arange(ant_names.shape[0])

    ant_id = ant_ids[ant_names == antenna][0]

    ff_tbl.open(caltable)
    antcol = ff_tbl.getcol('ANTENNA1')
    spwcol = ff_tbl.getcol('SPECTRAL_WINDOW_ID')
    ff_solution = ff_tbl.getcol('FPARAM')
    ff_tbl.close()

    selection = np.logical_and(antcol == ant_id, spwcol == spw)
    this_solution = ff_solution[:, 0, selection]
    ff_delay = 1e-9 * this_solution[1::4, 0]
    ff_phase = this_solution[0::4, 0]
    return ff_phase, ff_delay


def extract_fparam(caltable):
    ff_tbl = table()
    ff_tbl.open(caltable)
    solutions = ff_tbl.getcol('FPARAM')
    ff_tbl.close()
    return solutions

datapath = ctsys.resolve('unittest/pccor/')

class pccor_test_parse(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        for filename in test_data_files:
            shutil.copytree(os.path.join(datapath, filename), filename)
        return

    @classmethod
    def tearDownClass(cls):
        for filename in test_data_files:
            shutil.rmtree(filename)
        shutil.rmtree(def_ms+'.mpc', True)
        shutil.rmtree(def_pccor_caltable)
        return

    def test_parseAntenna(self):
        with self.assertRaises(KeyError):
            ret_dict = run_pccor(antenna=f'!{bad_ant}')
            ant_dict = ret_dict[f'ant_{bad_ant}']
        #self.assertTrue(passed, 'Return dict must not contain data for an excluded antenna')

    def test_parseAntenna_refant_excluded(self):
        with self.assertRaises(RuntimeError):
            run_pccor(antenna=f'!{def_refant}')
        #self.assertTrue(passed, 'pccor must throw RuntimeError if refant is excluded from processing antennas')

    def test_parseAntenna_antennas_specified(self):
        ant_list = ['LA', 'FD', 'MK']
        ret_dict = run_pccor(antenna=f','.join(ant_list))
        if len(ret_dict.keys()) == len(ant_list):
            n_correct = 0
            for ant in ant_list:
                if f'ant_{ant}' in ret_dict.keys():
                    n_correct += 1
            passed = n_correct == len(ant_list)
        else:
            passed = False
        self.assertTrue(passed, 'Return dict must contain all, and only the antennas specified in the input')

    def test_parseAntenna_mixes_excluded(self):
        with self.assertRaises(ValueError):
            run_pccor(antenna=f'!LA, FD')
        #self.assertTrue(passed, 'pccor must throw ValueError if user mixes excluding and adding antennas')

    def test_parseAntenna_nonexistent_antenna(self):
        with self.assertRaises(ValueError):
            ant_list = ['LA', 'FD', 'MK', '30m']
            run_pccor(antenna=f','.join(ant_list))
        #self.assertTrue(passed, 'pccor must throw ValueError if user wants to use a non existent antenna')

    def test_parseSpw(self):
        ret_dict = run_pccor(spw=f'!{bad_spw}')
        n_correct = 0
        for ant_key , ant_dict in ret_dict.items():
            try:
                ant_dict[f'spw_{bad_spw}']
            except KeyError:
                n_correct += 1

        passed = n_correct == len(ret_dict.keys())
        self.assertTrue(passed, 'Return dict must not contain data for an excluded SPW')

        spw_list = ['0', '2', '3']
        ret_dict = run_pccor(spw=f','.join(spw_list))
        n_correct = 0
        for ant_key , ant_dict in ret_dict.items():
            n_valid_spw = 0
            for spw in spw_list:
                try:
                    spw_dict = ant_dict[f'spw_{spw}']
                    n_valid_spw += 1
                except KeyError:
                    pass
            if n_valid_spw == len(spw_list):
                n_correct += 1
        passed = n_correct == len(ret_dict.keys())
        self.assertTrue(passed, 'Return dict must contain all, and only the SPWs specified in the input')

        try:
            run_pccor(spw=f'!0, 2')
            passed = False
        except ValueError:
            passed = True
        self.assertTrue(passed, 'pccor must throw ValueError if user mixes excluding and adding SPWs')

        try:
            run_pccor(spw='6')
            passed = False
        except ValueError:
            passed = True
        self.assertTrue(passed, 'pccor must throw ValueError if user wants to use a non existent SPW')
        return

    def test_parseScanAndTimerange_bad_timerange(self):
        with self.assertRaises(ValueError):
            run_pccor(timerange=bad_timerange)
        #self.assertTrue(passed, 'pccor must raise ValueError if timerange is outside observed times')

    def test_parseScanAndTimerange_incompatible_scan(self):
        with self.assertRaises(ValueError):
            run_pccor(timerange=def_range, scan=alt_scan)
        #self.assertTrue(passed, 'pccor must raise ValueError if timerange is incompatible with scan')

    def test_parseScanAndTimerange_bad_scan(self):
        with self.assertRaises(ValueError):
            run_pccor(scan=bad_scan)
        #self.assertTrue(passed, 'pccor must raise ValueError if scan is not present in data')

    def test_parseScanAndTimerange_invalid_scan(self):
        with self.assertRaises(ValueError):
            run_pccor(scan='32e')
        #self.assertTrue(passed, 'pccor must raise ValueError if scan is not a valid integer')

    def test_parseScanAndTimerange_invalid_timerange(self):
        with self.assertRaises(ValueError):
            run_pccor(timerange='asdrubal')
        #self.assertTrue(passed, 'pccor must raise ValueError if timerange is not a valid time range')

    def test_provideFringeFitTable(self):
        with self.assertRaises(RuntimeError):
            fringefit(vis=def_ms,
                      caltable=alt_ff_caltable,
                      selectdata=True,
                      timerange=alt_range,
                      solint='inf',
                      refant=def_refant,
                      zerorates=True,
                      minsnr=3.0,
                      globalsolve=True,
                      niter=100,
                      delaywindow=[-1e6, 1e6],
                      ratewindow=[-1e6, 1e6],
                      append=False,
                      corrdepflags=False,
                      corrcomb='none',
                      docallib=False,
                      paramactive=[True, True, False],
                      concatspws=True,
                      parang=False
                      )
            run_pccor(ff_table=alt_ff_caltable)
        #self.assertTrue(passed, 'pccor must raise RuntimeError if given fringefit caltable solution time is '
        #                        'incompatible with pccor timerange')

    def test_provideFringeFitTable_equal(self):
        fringefit(vis=def_ms,
          caltable=alt_ff_caltable,
          selectdata=True,
          timerange=alt_range,
          solint='inf',
          refant=def_refant,
          zerorates=True,
          minsnr=3.0,
          globalsolve=True,
          niter=100,
          delaywindow=[-1e6, 1e6],
          ratewindow=[-1e6, 1e6],
          append=False,
          corrdepflags=False,
          corrcomb='none',
          docallib=False,
          paramactive=[True, True, False],
          concatspws=True,
          parang=False
          )
        ret_dict = run_pccor(ff_table=alt_ff_caltable, timerange=alt_range, scan=missing_param)
        pccor_pt_sol = ret_dict[f'ant_{alt_ant}'][f'spw_{alt_spw}']
        ff_phase, ff_delay = extract_ff_solution(alt_ff_caltable, alt_ant, alt_spw)
        passed = np.all(ff_phase == pccor_pt_sol['ff_phase'])
        passed = passed and np.all(ff_delay == pccor_pt_sol['ff_delay'])
        self.assertTrue(passed, 'fringefit solutions in return dictionary must be equal to the ones in the provided '
                        'fringefit caltable')
        return

class pccor_test_solutionValues(unittest.TestCase):

    def setUp(self):
        shutil.copytree(os.path.join(datapath, def_ms), def_ms)
        shutil.copytree(os.path.join(datapath, ref_pccor_caltable), ref_pccor_caltable)
        return

    def tearDown(self):
        shutil.rmtree(def_ms)
        shutil.rmtree(ref_pccor_caltable)
        shutil.rmtree(def_ms+'.mpc', True)
        shutil.rmtree(def_pccor_caltable)
        return

    def test_solutionValues(self):
        run_pccor()
        current_solve = extract_fparam(def_pccor_caltable)
        reference_solve = extract_fparam(ref_pccor_caltable)
        tolerance = 1e-5
        passed = np.allclose(current_solve, reference_solve, rtol=tolerance, atol=tolerance)
        diff = current_solve - reference_solve
        diff_max = np.max(np.abs(diff))
        diff_mean = np.mean(diff)
        diff_median = np.median(diff)
        self.assertTrue(passed, f'Current solutions differs from reference solution. max diff: {diff_max}, mean diff: '
                                f'{diff_mean}, median diff {diff_median}, tol: {tolerance}.')

class pccor_test_missing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        for filename in test_data_files:
            shutil.copytree(os.path.join(datapath, filename), filename)

    @classmethod
    def tearDownClass(cls):
        for filename in test_data_files:
            shutil.rmtree(filename)
        shutil.rmtree(def_ms+'.mpc', True)
        shutil.rmtree(def_pccor_caltable)


    def test_missingPhaseCalSubtable(self):
        with self.assertRaises(RuntimeError):
            run_pccor(no_phase_cal_ms)
        #self.assertTrue(passed, 'pccor must throw an exception if no PHASE_CAL subtable is present')

    def test_missingPcDataUseFringefit(self):
        ret_data = run_pccor()
        ant_key = 'ant_FD'
        spw_key = 'spw_0'

        pccor_sol = ret_data[ant_key][spw_key]
        passed = pccor_sol['delay'].shape == (2, 1)
        self.assertTrue(passed, f'pccor must provide a single delay solution in time for {ant_key} {spw_key}')

        passed = np.all(pccor_sol['delay'] == pccor_sol['ff_delay'][:, np.newaxis])
        self.assertTrue(passed, 'pccor delay solution must be equal to fringefit solution')

        passed = np.all(pccor_sol['phase'] == pccor_sol['ff_phase'][:, np.newaxis])
        self.assertTrue(passed, 'pccor phase solution must be equal to fringefit solution')


    def test_missingPcDataForRefant(self):
        with self.assertRaises(RuntimeError):
            run_pccor(refant=bad_ant)
        #self.assertTrue(passed, 'pccor must throw RuntimeError when there is no refant pc data')


class pccor_test_splitMses(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        for filename in [contiguous_ms, non_contiguous_ms]:
            shutil.copytree(os.path.join(datapath, filename), filename)
        return

    @classmethod
    def tearDownClass(cls):
        for filename in [contiguous_ms, non_contiguous_ms]:
            shutil.rmtree(filename)
        shutil.rmtree(contiguous_ms+'.mpc', True)
        shutil.rmtree(non_contiguous_ms+'.mpc', True)

    def test_splitMses_contiguous_ms(self):
        try:
            run_pccor(vis=contiguous_ms)
            passed = True
        except:
            passed = False
        self.assertTrue(passed, 'pccor should not throw exceptions with a contigous split ms')

    def test_splitMses_non_contiguous_ms(self):
        try:
            run_pccor(vis=non_contiguous_ms)
            passed = True
        except:
            passed = False
        self.assertTrue(passed, 'pccor should not throw exceptions with a non contigous split ms')


if __name__ == '__main__':
    unittest.main()
