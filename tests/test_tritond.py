from testify import *

import os
import subprocess

class Tritond(TestCase):

    def test_help(self):
        """
        Tritond should display help without issue.
        """
        with open(os.devnull, "wb") as null:
            cmd = 'python ./bin/tritond --help'
            p = subprocess.Popen(cmd.split(' '), stdout=null, stderr=null)
            p.communicate()
            assert_equal(p.returncode, 0)
