from typing import Dict, Any, List
from datetime import datetime
from dataclasses import dataclass
from datetime import UTC

@dataclass
class Checkpoint:
    """Represents a checkpoint in the system."""
    
    id: str
    """The unique identifier for the checkpoint."""
    
    data: Dict[str, Any]
    """The data stored in the checkpoint."""
    
    description: str
    """A description of the checkpoint."""
    
    @classmethod
    def from_prosci(cls, checkpoint_id: str, data: Dict[str, Any], description: str) -> 'Checkpoint':
        """Create a checkpoint from ProSci format.
        
        Args:
            checkpoint_id: The ID of the checkpoint
            data: The data to store
            description: A description of the checkpoint
            
        Returns:
            A new Checkpoint instance
        """
        return cls(
            id=checkpoint_id,
            data=data,
            description=description
        )

@dataclass
class CheckpointMetadata:
    """Metadata associated with a checkpoint."""
    
    checkpoint_id: str
    """The ID of the checkpoint this metadata belongs to."""
    
    timestamp: datetime
    """When the checkpoint was created."""
    
    version: str
    """The version of the checkpoint."""
    
    description: str
    """A description of the checkpoint."""
    
    tags: List[str]
    """Tags associated with the checkpoint."""
    
    metadata: Dict[str, Any]
    """Additional metadata.""" 