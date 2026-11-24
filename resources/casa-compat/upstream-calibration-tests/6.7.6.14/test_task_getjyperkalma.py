import csv
import os
import shutil
import unittest
from unittest.mock import patch
from casatestutils import testhelper as th
import uuid
import json

from casatools import ctsys, table
from casatasks import getjyperkalma

_tb = table()
datapath = ctsys.resolve('/unittest/gencal/')


class TestGetJyPerKALMA(unittest.TestCase):

    vis = 'uid___A002_X85c183_X36f.ms'
    jyperk_factor_csv = os.path.join(datapath, 'jyperk_factor.csv')
    INVALID_URL_ENV = "https://invalid-host-for-test.example/jy"
    URL_BACKUP1 = "https://wrong.backup1.url/jy"
    URL_VALID_JAO = "https://asa.alma.cl/science/jy-kelvins"
    fake_response_dict = {
        "success": False,
        "error": "expected error"
    }
    fake_response = json.dumps(fake_response_dict)

    @classmethod
    def setUpClass(cls):
        cls.cwd = os.getcwd()
        cls.workdir = f'work_{uuid.uuid4()}'
        os.mkdir(cls.workdir)
        os.chdir(cls.workdir)

        shutil.copytree(
            os.path.join(datapath, f'{cls.vis}.sel'),
            cls.vis,
            symlinks=False
        )

    @classmethod
    def tearDownClass(cls):
        os.chdir(cls.cwd)
        shutil.rmtree(cls.workdir)

    def setUp(self):
        self.caltable = f'jyperk_{uuid.uuid4()}.cal'

    def tearDown(self):
        if os.path.isdir(self.caltable):
            shutil.rmtree(self.caltable)

    @classmethod
    def _load_jyperkdb_responses(cls, test_data):
        responses = {}
        with open(test_data) as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                responses[row[0]] = row[1]
        return responses

    def test_invalid_endpoint(self):
        with self.assertRaises(ValueError):
            getjyperkalma(
                vis=self.vis,
                caltable=self.caltable,
                endpoint='bad'
            )

    def test_missing_vis(self):
        with self.assertRaises(AssertionError):
            getjyperkalma(
                vis='no_such.ms',
                caltable=self.caltable
            )

    def test_not_vis_name_in_factor_csv(self):
        """
        Verify that no calibration table is generated when the
        Measurement Set name is not present in the Jy/K factor CSV file.
        """

        vis = 'non-existent-observation.ms'
        if not os.path.isfile(vis):
            os.symlink(self.vis, vis)

        with self.assertRaises(Exception) as cm:
            getjyperkalma(vis=vis,
                          caltable=self.caltable,
                          infile=self.jyperk_factor_csv)

        self.assertEqual(cm.exception.args[0],
                         'No valid Jy/K factors found for this MS')

    @patch.dict(os.environ, {"JYPERKDB_URL": URL_VALID_JAO})
    @patch("casatasks.private.jyperk.JyPerKDatabaseClient._try_to_get_response")
    def test_jyperk_env_var_OK(self, mock_retrieve):
        """
        Test that web API error raises RuntimeError.
        JYPERKDB_URL is set and must be used.
        """

        mock_retrieve.return_value = self.fake_response

        with self.assertRaisesRegex(
            RuntimeError,
            "No Jy/K factors available: JYPERKDB_URL, backup_hosts, and infile all failed"
        ):
            getjyperkalma(vis=self.vis,
                          caltable=self.caltable,)

        self.assertTrue(mock_retrieve.called)
        self.assertTrue(mock_retrieve.mock_calls[0].startswith(self.URL_VALID_JAO))

    @patch.dict(os.environ, {"JYPERKDB_URL": URL_VALID_JAO})
    @patch("casatasks.private.jyperk.JyPerKDatabaseClient._try_to_get_response")
    def test_jyperk_env_var_OK_backup_available(self, mock_retrieve):
        """
        Test that web API error raises RuntimeError.
        JYPERKDB_URL and backup host are set, must use first one.
        """

        mock_retrieve.return_value = self.fake_response

        with self.assertRaisesRegex(
            RuntimeError,
            "No Jy/K factors available: JYPERKDB_URL, backup_hosts, and infile all failed"
        ):
            getjyperkalma(vis=self.vis, caltable=self.caltable,
                          backup_hosts=[self.URL_BACKUP1])

        self.assertTrue(mock_retrieve.mock_calls[0].startswith(self.URL_VALID_JAO))

    @patch.dict(os.environ, {"JYPERKDB_URL": " "})
    @patch("casatasks.private.jyperk.casalog.post")
    @patch("casatasks.private.jyperk.JyPerKDatabaseClient._try_to_get_response")
    def test_jyperk_NO_env_var_backup_OK(self, mock_retrieve, mock_casalog):
        """
        Test that web API error raises RuntimeError.
        JYPERKDB_URL is empty, but backup hosts is set.
        Check that it proceeds with backup host.
        """

        mock_retrieve.return_value = self.fake_response

        with self.assertRaisesRegex(
            RuntimeError,
            "No Jy/K factors available: JYPERKDB_URL, backup_hosts, and infile all failed"
        ):
            getjyperkalma(vis=self.vis, caltable=self.caltable,
                          backup_hosts=[
                            'https://almascience.nrao.edu/science/jy-kelvins'])
        self.assertTrue(
            any(
                "JYPERKDB_URL is set but empty" in args[0]
                for args, _ in mock_casalog.call_args_list
            )
        )
        self.assertTrue(
            any(
                "Failed Jy/K DB backup host" in args[0]
                for args, _ in mock_casalog.call_args_list
            )
        )
        self.assertTrue(mock_retrieve.call_count == 1)
        self.assertTrue(mock_retrieve.mock_calls[0].startswith(self.URL_VALID_JAO))

    @patch.dict(os.environ, {"JYPERKDB_URL": INVALID_URL_ENV})
    @patch("casatasks.private.jyperk.casalog.post")
    @patch("casatasks.private.jyperk.JyPerKDatabaseClient._try_to_get_response")
    def test_getjyperkalma_all_failed(self, mock_retrieve, mock_casalog):
        """
        Test to check all failed situation.
        """

        mock_retrieve.return_value = self.fake_response

        with self.assertRaisesRegex(
            RuntimeError,
            "No Jy/K factors available: JYPERKDB_URL, backup_hosts, and infile all failed"
        ):
            getjyperkalma(vis=self.vis,
                          caltable=self.caltable,
                          backup_hosts=[
                            'https://almascience.nrao.edu/science/jy'])
        self.assertTrue(
            any(
                "Failed to access all backup hosts." in args[0]
                for args, _ in mock_casalog.call_args_list
            )
        )

    @patch.dict(os.environ, {"JYPERKDB_URL": INVALID_URL_ENV})
    @patch("casatasks.private.jyperk.JyPerKDatabaseClient._try_to_get_response")
    def test_getjyperkalma_env_var_NG_backup_called(self, mock_retrieve):
        """
        JYPERKDB_URL fails, proceed with backup host.
        """

        mock_retrieve.return_value = self.fake_response

        with self.assertRaisesRegex(
            RuntimeError,
            "No Jy/K factors available: JYPERKDB_URL, backup_hosts, and infile all failed"
        ):
            getjyperkalma(vis=self.vis,
                          caltable=self.caltable,
                          backup_hosts=[self.URL_VALID_JAO])
        self.assertTrue(mock_retrieve.call_count == 2)
        self.assertTrue(mock_retrieve.mock_calls[0].startswith(self.INVALID_URL_ENV))
        self.assertTrue(mock_retrieve.mock_calls[1].startswith(self.URL_VALID_JAO))

    @patch.dict(os.environ, {"JYPERKDB_URL": INVALID_URL_ENV})
    @patch("casatasks.private.jyperk.JyPerKDatabaseClient._try_to_get_response")
    def test_getjyperkalma_env_var_NG_backup2_success(self, mock_retrieve):
        """
        Test JYPERKDB_URL failed, first backup host failed, second succeeded.
        JYPERKDB_URL is set incorrect
        """

        responses = self._load_jyperkdb_responses(
                os.path.join(datapath, 'jyperk_web_api_response/asdm.csv'))

        def get_response(url):
            if url.startswith(self.INVALID_URL_ENV):
                return '{"success": false, "error": "env failed"}'
            elif url.startswith(self.URL_BACKUP1):
                return '{"success": false, "error": "backup1 failed"}'
            elif url.startswith(self.URL_VALID_JAO):
                return responses[url]
            else:
                raise ValueError(f"Unexpected URL: {url}")

        mock_retrieve.side_effect = get_response

        getjyperkalma(vis=self.vis, caltable=self.caltable,
                      backup_hosts=[self.URL_BACKUP1, self.URL_VALID_JAO])

        reference_caltable = os.path.join(
                datapath, 'jyperk_reference/web_api_with_asdm.cal')

        self.assertTrue(os.path.exists(self.caltable))
        self.assertTrue(th.compTables(self.caltable,
                                      reference_caltable, ['WEIGHT']))

        self.assertTrue(mock_retrieve.call_count == 3)
        self.assertTrue(mock_retrieve.mock_calls[0].startswith(self.INVALID_URL_ENV))
        self.assertTrue(mock_retrieve.mock_calls[1].startswith(self.URL_BACKUP1))
        self.assertTrue(mock_retrieve.mock_calls[2].startswith(self.URL_VALID_JAO))


if __name__ == '__main__':
    unittest.main()
