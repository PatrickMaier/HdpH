-- Message passing via MPI; wrapper module
--
-- Visibility: external
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 29 May 2011
--
-----------------------------------------------------------------------------

module MP.MPI
  ( -- re-export one of the following
    module MP.MPI_ByteString
  ) where

-- import what's re-exported above
import MP.MPI_ByteString
