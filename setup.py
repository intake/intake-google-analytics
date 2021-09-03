from setuptools import setup
import versioneer

requirements = [
    # package requirements go here
]

setup(
    name='intake-google-analytics',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Driver for Google Analytics queries",
    license="BSD",
    author="Albert DeFusco",
    author_email='adefusco@anaconda.com',
    url='https://github.com/Anaconda/intake-google-analytics',
    packages=['intake_google_analytics'],
    
    install_requires=requirements,
    keywords='intake-google-analytics',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
