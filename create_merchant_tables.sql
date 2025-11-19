-- SQL script to create merchants and onboarding_jobs tables
-- Run this to set up the necessary tables for merchant onboarding

-- Create merchants table
CREATE TABLE IF NOT EXISTS merchants (
    merchant_id VARCHAR(255) PRIMARY KEY,
    user_id TEXT NOT NULL,
    shop_name VARCHAR(255) NOT NULL,
    shop_url VARCHAR(255),
    bot_name VARCHAR(255) DEFAULT 'AI Assistant',
    target_customer TEXT,
    top_questions TEXT,
    top_products TEXT,
    primary_color VARCHAR(50) DEFAULT '#667eea',
    secondary_color VARCHAR(50) DEFAULT '#764ba2',
    logo_url TEXT,
    status VARCHAR(50) DEFAULT 'active',
    vertex_datastore_id VARCHAR(255),
    config_path TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- Create indexes for merchants
CREATE INDEX IF NOT EXISTS idx_merchants_user_id ON merchants(user_id);
CREATE INDEX IF NOT EXISTS idx_merchants_status ON merchants(status);
CREATE INDEX IF NOT EXISTS idx_merchants_created_at ON merchants(created_at);

-- Create onboarding_jobs table
CREATE TABLE IF NOT EXISTS onboarding_jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    merchant_id VARCHAR(255) NOT NULL,
    user_id TEXT NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    progress INTEGER DEFAULT 0,
    total_steps INTEGER DEFAULT 6,
    current_step VARCHAR(100),
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    
    FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- Create indexes for onboarding_jobs
CREATE INDEX IF NOT EXISTS idx_onboarding_jobs_merchant_id ON onboarding_jobs(merchant_id);
CREATE INDEX IF NOT EXISTS idx_onboarding_jobs_user_id ON onboarding_jobs(user_id);
CREATE INDEX IF NOT EXISTS idx_onboarding_jobs_status ON onboarding_jobs(status);
CREATE INDEX IF NOT EXISTS idx_onboarding_jobs_created_at ON onboarding_jobs(created_at);

-- Create vertex_datastores table (optional, for tracking Vertex AI Search setup)
CREATE TABLE IF NOT EXISTS vertex_datastores (
    merchant_id VARCHAR(255) PRIMARY KEY,
    datastore_id VARCHAR(255) NOT NULL UNIQUE,
    project_id VARCHAR(255) NOT NULL,
    location VARCHAR(100) NOT NULL DEFAULT 'global',
    collection_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'creating',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id) ON DELETE CASCADE
);

-- Create index for vertex_datastores
CREATE INDEX IF NOT EXISTS idx_vertex_datastores_datastore_id ON vertex_datastores(datastore_id);

