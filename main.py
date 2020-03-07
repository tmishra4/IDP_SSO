import sys, os

try:
    raise FileNotFoundError("No error")
except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    str = str(exc_type)+ "   " + str(exc_obj)+ "   " + str(sys.exc_info())
    print(sys.exc_info())
    print(sys.exc_info()[1])
    print(sys.exc_info()[1])
