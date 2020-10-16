Docker Usage
============
As a convenience, Bunsen offers a Docker image based on the
`Jupyter Docker Stacks <http://jupyter-docker-stacks.readthedocs.io/en/latest/>`_
project. Users can use it by grabbing the following image and running it:

>>> docker pull cerner/bunsen-notebook
>>>
>>> docker run -p 8888:8888 cerner/bunsen-notebook

You then can simply click on the link printed in the console, and a Jupyter session
will open that includes the Bunsen Python APIs and supporting libraries. A common
variation of this is to mount a directory on the host computer that contains local
notebooks or data:

>>> docker run -p 8888:8888 -v $PWD:/home/jovyan/work cerner/bunsen-notebook

This will place a view of the current directory in the "work" folder seen in Jupyter.

See the `documentation in the Jupyter Docker Stacks <http://jupyter-docker-stacks.readthedocs.io/en/latest/using/running.html>`_
project for additional configuration options.
