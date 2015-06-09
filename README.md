Triton Project
=========================

Data pipeline for Postmates


Development
---

You should be able to configuration your development environment using make:

    ~/Projects/Project $ make dev

The tests should all work:

    ~/Projects/Project $ make test
    .
    PASSED.  1 test / 1 case: 1 passed, 0 failed.  (Total test time 0.00s)

If you need to debug your application with ipython:

    ~/Projects/Project $ make shell
    Python 2.7.3 (default, Apr 27 2012, 21:31:10) 
    Type "copyright", "credits" or "license" for more information.

    IPython 0.12.1 -- An enhanced Interactive Python.
    ?         -> Introduction and overview of IPython's features.
    %quickref -> Quick reference.
    help      -> Python's own help system.
    object?   -> Details about 'object', use 'object??' for extra details.

    In [1]: from project.models import Project

    In [2]:
