from setuptools import setup
import versioneer

with open("README.md", "r") as fh:
    long_description = fh.read()

requirements = [
    'intake',
    'pandas',
    'google-api-python-client',
    'google-auth-oauthlib'
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
        entry_points={
        'intake.drivers': [
            'google_analytics_query = intake_google_analytics.source:GoogleAnalyticsQuerySource',
        ]
    },
    install_requires=requirements,
    keywords='intake-google-analytics',
    classifiers=[
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    long_description=long_description,
    long_description_content_type='text/markdown'
)
