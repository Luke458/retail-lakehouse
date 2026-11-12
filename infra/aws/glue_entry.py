"""AWS Glue entry point: the job's script (infra/aws/glue.tf).

The job's --additional-python-modules installs the retail_lakehouse wheel, so
the logic lives in the package (retail_lakehouse.glue), where it is tested.
"""

import sys

from retail_lakehouse.glue import run

run(sys.argv[1:])
