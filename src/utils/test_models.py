#!/usr/bin/python3
import pytest
from utils.models import Password

def test_password_env(monkeypatch):
    monkeypatch.setenv("PASSWORD", "foobarbaz")
    result = str(Password(Password.DEFAULT))
    assert result == "foobarbaz"
    result = str(Password("foobaz")) 
    assert result == "foobaz"
