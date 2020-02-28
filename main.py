# -*- encoding: utf-8 -*-

import sys
from MyForm import *

if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = MyForm()
    w.show()
    sys.exit(app.exec_())