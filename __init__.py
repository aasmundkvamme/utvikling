try:
    import sys
    import os
    sys.path.append(os.path.abspath('../test_a/test_a_a'))
    import akvut
    akvut.test()
except ImportError:
    print("Error: Could not import akvut")