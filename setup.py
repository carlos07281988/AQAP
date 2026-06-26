from setuptools import setup

setup(
    name="aqap",
    version="1.0.0",
    packages=[
        "aqap",
        "aqap.core",
        "aqap.agent",
        "aqap.plugin",
        "aqap.plugins",
        "aqap.transport",
    ],
    install_requires=[
        "pyyaml>=6.0",
        "redis>=5.0",
        "cryptography>=40.0",
    ],
    extras_require={
        "kafka": ["kafka-python>=2.0"],
        "dev": ["pytest>=7.0", "pytest-asyncio>=0.21", "pytest-cov>=4.0"],
    },
    python_requires=">=3.9",
)
