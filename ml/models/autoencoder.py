import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
from typing import Dict, Any, Union

class AutoencoderArchitecture(nn.Module):
    def __init__(self, input_dim: int) -> None:
        super(AutoencoderArchitecture, self).__init__()
        # Architecture: 512 -> 256 -> 128 -> 256 -> 512
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 512),
            nn.ReLU(),
            nn.Linear(512, 256),
            nn.ReLU(),
            nn.Linear(256, 128),
            nn.ReLU()
        )
        self.decoder = nn.Sequential(
            nn.Linear(128, 256),
            nn.ReLU(),
            nn.Linear(256, 512),
            nn.ReLU(),
            nn.Linear(512, input_dim)
            # Omitting sigmoid so it works well with StandardScaler (unbounded)
        )
        
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return decoded

class Autoencoder:
    def __init__(self, input_dim: int) -> None:
        self.input_dim = input_dim
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = AutoencoderArchitecture(input_dim).to(self.device)
        self.threshold: float = 0.0
        self.is_trained: bool = False
        
    def train(self, X: np.ndarray, epochs: int = 50, lr: float = 1e-3, patience: int = 5) -> None:
        """Trains the autoencoder with early stopping."""
        criterion = nn.MSELoss()
        optimizer = optim.Adam(self.model.parameters(), lr=lr)
        
        X_tensor = torch.FloatTensor(X).to(self.device)
        dataset = torch.utils.data.TensorDataset(X_tensor, X_tensor)
        dataloader = torch.utils.data.DataLoader(dataset, batch_size=256, shuffle=True)
        
        best_loss = float("inf")
        patience_counter = 0
        
        print("Starting Autoencoder training...")
        for epoch in range(epochs):
            self.model.train()
            epoch_loss = 0.0
            
            for batch_x, _ in dataloader:
                optimizer.zero_grad()
                outputs = self.model(batch_x)
                loss = criterion(outputs, batch_x)
                loss.backward()
                optimizer.step()
                epoch_loss += loss.item()
                
            avg_loss = epoch_loss / len(dataloader)
            
            # Early stopping check
            if avg_loss < best_loss:
                best_loss = avg_loss
                patience_counter = 0
            else:
                patience_counter += 1
                
            if patience_counter >= patience:
                print(f"Early stopping triggered at epoch {epoch+1}")
                break
                
        # Calculate dynamic threshold on training set
        self.model.eval()
        with torch.no_grad():
            outputs = self.model(X_tensor)
            recon_errors = torch.mean((outputs - X_tensor) ** 2, dim=1).cpu().numpy()
            # Threshold = mean + 3 * std
            self.threshold = float(np.mean(recon_errors) + 3 * np.std(recon_errors))
            
        self.is_trained = True
        print(f"Training completed. Determined anomaly threshold: {self.threshold:.4f}")

    def predict(self, X: np.ndarray) -> Dict[str, Union[float, bool, list]]:
        """Predicts anomalies using reconstruction error."""
        if not self.is_trained:
            raise ValueError("Model is not trained.")
            
        self.model.eval()
        X_tensor = torch.FloatTensor(X).to(self.device)
        
        with torch.no_grad():
            outputs = self.model(X_tensor)
            recon_errors = torch.mean((outputs - X_tensor) ** 2, dim=1).cpu().numpy()
            
        is_anomaly = recon_errors > self.threshold
        
        return {
            "score": float(recon_errors[0]) if len(recon_errors) == 1 else recon_errors.tolist(),
            "is_anomaly": bool(is_anomaly[0]) if len(is_anomaly) == 1 else is_anomaly.tolist()
        }
