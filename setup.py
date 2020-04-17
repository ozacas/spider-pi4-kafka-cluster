import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ozacas", 
    version="0.1.0",
    author="ozacas",
    author_email="https://github.com/ozacas",
    description="Example spider using kafka as ESB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ozacas/kafkaspider",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    package_dir={'': 'src'},  
)

