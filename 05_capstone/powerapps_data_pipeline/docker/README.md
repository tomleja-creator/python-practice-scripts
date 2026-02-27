# Docker Deployment for PowerApps ETL Pipeline

## 🐳 Overview

This directory contains Docker configurations for containerizing the PowerApps ETL Pipeline. Containerization ensures consistent behavior across environments and simplifies deployment.

## 📋 Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- 2GB RAM minimum (4GB recommended)
- Git (for cloning)

## 🚀 Quick Start

### 1. Clone and Setup
```bash
cd powerapps_data_pipeline/docker
make init  # Creates .env file from template
