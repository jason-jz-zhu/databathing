import setuptools

setuptools.setup(
    name='databathing',
    version='0.2.1',
    description="build spark job based on query",
    author="Jiazhen Zhu",
    author_email="jason.jz.zhu@gmail.com",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: SQL","Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        'License :: OSI Approved :: MIT License'],
    license="MIT",
    packages=['databathing'],
    install_requires=[
          'mo-sql-parsing',
      ],
    long_description='# Convert SQL to Spark Code!\n\n[![PyPI Latest Release]',
    long_description_content_type='text/markdown'
)
