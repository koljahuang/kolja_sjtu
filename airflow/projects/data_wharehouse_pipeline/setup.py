from setuptools import setup, find_packages

setup(
    name="data_wharehouse_sink_2_ods_pipline",
    version="0.0.1",
    author="黄超",
    author_email="koljahuang@stju.edu.cn ",
    description="data wharehouse sink to ods pipline",

    classifiers = [
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',

        'Topic :: Software Development :: Etl Tools',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3.8',
    ],

    py_modules = ['config'], 
    
    packages=find_packages(),
    
    install_requires=['dynaconf>=3.1'],

    package_data = {'': ['configs/.secrets_dev.toml', 
                         'configs/.secrets.toml',
                         'configs/*.toml']}
)