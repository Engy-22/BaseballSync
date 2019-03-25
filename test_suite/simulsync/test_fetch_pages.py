import os
import sys

sys.path.append(os.path.join(sys.path[0], '..', '..'))
print(sys.path)

from src.simulsync_service import driver


print(driver)
