
.. |logo_image| image:: https://github.com/3dem/emhub/wiki/images/emwrap-logo.png
   :height: 60px

|logo_image|

Python wrappers for CryoEM/CryoET programs that can be executed from the command line. The wrappers have been 
designed to be compatible with the definition of external jobs in Relion. They have only two arguments: input JSON values 
with key=value pairs and the output folder. The processing workflow can be launched and monitored through the EMhub web interface. 

**emwrap** is the backend processing library used by `EMhub-Tomo <https://3dem.github.io/emhub-tomo/>`_: EMhub-Tomo's web interface
launches and monitors CryoET jobs by calling the wrappers defined in this package. For the full documentation, covering installation,
configuration, and the available processing jobs, see the `EMhub-Tomo documentation <https://3dem.github.io/emhub-tomo/>`_.

