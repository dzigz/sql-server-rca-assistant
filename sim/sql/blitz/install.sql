-- ============================================================================
-- Blitz Scripts Installation
-- First Responder Kit from https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit
-- ============================================================================
-- Version: 8.28 (2025-11-24)
-- Downloaded: 2026-01-01
-- License: MIT
-- ============================================================================

USE master;
GO

-- Blitz scripts require these SET options for indexed views and computed columns
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT 'Installing Blitz scripts from First Responder Kit...';
PRINT 'Version: 8.28 (2025-11-24)';
GO

-- ============================================================================
-- Install sp_BlitzFirst - Real-time performance diagnostics
-- ============================================================================
PRINT 'Installing sp_BlitzFirst...';
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO
:r sp_BlitzFirst.sql
GO

-- ============================================================================
-- Install sp_BlitzCache - Query plan cache analysis
-- ============================================================================
PRINT 'Installing sp_BlitzCache...';
GO
:r sp_BlitzCache.sql
GO

-- ============================================================================
-- Install sp_BlitzWho - Active session monitoring
-- ============================================================================
PRINT 'Installing sp_BlitzWho...';
GO
:r sp_BlitzWho.sql
GO

-- ============================================================================
-- Install sp_BlitzIndex - Index analysis
-- ============================================================================
PRINT 'Installing sp_BlitzIndex...';
GO
:r sp_BlitzIndex.sql
GO

-- ============================================================================
-- Install sp_BlitzLock - Deadlock analysis
-- ============================================================================
PRINT 'Installing sp_BlitzLock...';
GO
:r sp_BlitzLock.sql
GO

-- ============================================================================
-- Install sp_Blitz - Server health check
-- ============================================================================
PRINT 'Installing sp_Blitz...';
GO
:r sp_Blitz.sql
GO

-- ============================================================================
-- Verify installation
-- ============================================================================
PRINT 'Verifying installation...';
GO

SELECT
    name AS ProcedureName,
    create_date AS CreatedDate,
    modify_date AS ModifiedDate
FROM sys.procedures
WHERE name IN ('sp_Blitz', 'sp_BlitzFirst', 'sp_BlitzCache', 'sp_BlitzWho', 'sp_BlitzIndex', 'sp_BlitzLock')
ORDER BY name;
GO

PRINT 'Blitz scripts installation complete!';
GO
