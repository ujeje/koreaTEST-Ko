from setuptools import setup, find_packages

setup(
    name="stock_trader",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'numpy',
        'pandas',
        'pyyaml',
        'requests',
        'pytz',
        'google-api-python-client',
        'google-auth-httplib2',
        'google-auth-oauthlib',
        'discord-webhook',
        'python-dateutil',
        'schedule'
    ],
) 