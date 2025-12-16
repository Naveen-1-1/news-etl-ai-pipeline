#!/usr/bin/env python3
"""
Create Airflow admin user with fixed credentials for Docker
"""
import os
import sys

os.environ.setdefault('AIRFLOW_HOME', '/opt/airflow')

from airflow.models import User
from airflow import settings

try:
    session = settings.Session()
    
    # Check if admin exists
    existing = session.query(User).filter(User.username == 'admin').first()
    
    if not existing:
        user = User(
            username='admin',
            email='admin@example.com',
            firstname='Admin',
            lastname='User',
            role='Admin',
            password='admin'
        )
        session.add(user)
        session.commit()
        print("✅ Admin user created: admin/admin")
    else:
        print("✅ Admin user already exists")
    
    session.close()
except Exception as e:
    print(f"❌ Error creating user: {e}")
    sys.exit(1)
