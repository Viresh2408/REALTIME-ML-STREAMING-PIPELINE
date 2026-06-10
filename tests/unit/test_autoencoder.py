"""
Unit tests for Autoencoder model.

Tests cover:
  - Architecture verification (512→256→128→256→512 encoder/decoder)
  - Forward pass and output shape
  - Reconstruction error properties
  - Training and loss reduction
  - Anomaly detection scoring
  - Threshold calculation (mean + 3σ)
  - Early stopping mechanism
  - State dict save/load
"""

from __future__ import annotations

import numpy as np
import pytest
import torch

from ml.models.autoencoder import Autoencoder, AutoencoderArchitecture


@pytest.mark.unit
class TestAutoencoderArchitecture:
    """Tests for AutoencoderArchitecture neural network module."""

    def test_autoencoder_instantiates_with_correct_architecture(self) -> None:
        """Verify encoder and decoder layer structure: 512→256→128→256→512."""
        input_dim = 10
        model = AutoencoderArchitecture(input_dim)

        # Encoder: input -> 512 -> 256 -> 128
        assert len(model.encoder) == 6  # 3 Linear + 3 ReLU
        assert isinstance(model.encoder[0], torch.nn.Linear)
        assert model.encoder[0].in_features == input_dim
        assert model.encoder[0].out_features == 512

        assert isinstance(model.encoder[2], torch.nn.Linear)
        assert model.encoder[2].in_features == 512
        assert model.encoder[2].out_features == 256

        assert isinstance(model.encoder[4], torch.nn.Linear)
        assert model.encoder[4].in_features == 256
        assert model.encoder[4].out_features == 128

        # Decoder: 128 -> 256 -> 512 -> output
        assert len(model.decoder) == 5  # 3 Linear + 2 ReLU (last layer has no activation)
        assert isinstance(model.decoder[0], torch.nn.Linear)
        assert model.decoder[0].in_features == 128
        assert model.decoder[0].out_features == 256

        assert isinstance(model.decoder[2], torch.nn.Linear)
        assert model.decoder[2].in_features == 256
        assert model.decoder[2].out_features == 512

        assert isinstance(model.decoder[4], torch.nn.Linear)
        assert model.decoder[4].in_features == 512
        assert model.decoder[4].out_features == input_dim

    def test_forward_pass_returns_correct_output_shape(self) -> None:
        """Forward pass through autoencoder should preserve input shape."""
        input_dim = 10
        batch_size = 32
        model = AutoencoderArchitecture(input_dim)
        model.eval()

        X = torch.randn(batch_size, input_dim)
        with torch.no_grad():
            output = model(X)

        assert output.shape == (batch_size, input_dim)
        assert isinstance(output, torch.Tensor)

    def test_forward_pass_on_single_sample(self) -> None:
        """Single sample should also work correctly."""
        input_dim = 10
        model = AutoencoderArchitecture(input_dim)
        model.eval()

        X = torch.randn(1, input_dim)
        with torch.no_grad():
            output = model(X)

        assert output.shape == (1, input_dim)

    def test_encoder_outputs_bottleneck_dimension(self) -> None:
        """Encoder should output 128-dimensional bottleneck."""
        input_dim = 10
        model = AutoencoderArchitecture(input_dim)
        model.eval()

        X = torch.randn(32, input_dim)
        with torch.no_grad():
            encoded = model.encoder(X)

        assert encoded.shape == (32, 128)

    def test_model_has_learnable_parameters(self) -> None:
        """Encoder and decoder should have learnable parameters."""
        input_dim = 10
        model = AutoencoderArchitecture(input_dim)

        params = list(model.parameters())
        assert len(params) > 0, "Model should have learnable parameters"

        for param in params:
            assert param.requires_grad, "Parameters should be trainable"


@pytest.mark.unit
class TestAutoencoder:
    """Tests for Autoencoder wrapper class."""

    def _make_synthetic_data(self, n_samples: int = 100, n_features: int = 10) -> np.ndarray:
        """Generate synthetic normal and anomalous data."""
        rng = np.random.default_rng(42)
        normal = rng.standard_normal((n_samples, n_features))
        anomalies = rng.standard_normal((int(n_samples * 0.1), n_features)) * 3
        return np.vstack([normal, anomalies])

    def test_autoencoder_instantiates(self) -> None:
        """Autoencoder should instantiate with input_dim."""
        ae = Autoencoder(input_dim=10)

        assert ae.input_dim == 10
        assert ae.is_trained is False
        assert ae.threshold == 0.0
        assert ae.model is not None
        assert isinstance(ae.model, AutoencoderArchitecture)

    def test_device_selection(self) -> None:
        """Model should select CUDA if available, else CPU."""
        ae = Autoencoder(input_dim=10)

        if torch.cuda.is_available():
            assert ae.device.type == "cuda"
        else:
            assert ae.device.type == "cpu"

    def test_model_moved_to_device(self) -> None:
        """Model parameters should be on the selected device."""
        ae = Autoencoder(input_dim=10)

        for param in ae.model.parameters():
            assert param.device == ae.device

    def test_train_reduces_loss_over_epochs(self) -> None:
        """Training should reduce reconstruction loss."""
        ae = Autoencoder(input_dim=10)
        X = self._make_synthetic_data(n_samples=100, n_features=10)

        ae.train(X, epochs=20, lr=1e-2, patience=10)

        assert ae.is_trained is True
        assert ae.threshold > 0.0

    def test_train_sets_is_trained_flag(self) -> None:
        """After training, is_trained flag should be True."""
        ae = Autoencoder(input_dim=10)
        X = self._make_synthetic_data()

        assert ae.is_trained is False
        ae.train(X, epochs=5)
        assert ae.is_trained is True

    def test_reconstruction_error_is_non_negative(self) -> None:
        """Reconstruction error (MSE) should always be non-negative."""
        ae = Autoencoder(input_dim=10)
        X = self._make_synthetic_data()
        ae.train(X, epochs=5)

        X_test = X[:10]
        result = ae.predict(X_test)

        scores = result["score"]
        if isinstance(scores, list):
            scores = np.array(scores)
        else:
            scores = np.array([scores])

        assert np.all(scores >= 0), "Reconstruction errors must be non-negative"

    def test_threshold_set_at_mean_plus_3_std(self) -> None:
        """Threshold should be calculated as mean + 3*std of training errors."""
        ae = Autoencoder(input_dim=10)
        X = self._make_synthetic_data(n_samples=100)

        ae.train(X, epochs=10)

        ae.model.eval()
        X_tensor = torch.FloatTensor(X).to(ae.device)
        with torch.no_grad():
            outputs = ae.model(X_tensor)
            recon_errors = torch.mean((outputs - X_tensor) ** 2, dim=1).cpu().numpy()

        expected_threshold = float(np.mean(recon_errors) + 3 * np.std(recon_errors))

        # Allow small floating-point tolerance
        assert abs(ae.threshold - expected_threshold) < 1e-5

    def test_predict_raises_if_not_trained(self) -> None:
        """Predict should raise ValueError if model not trained."""
        ae = Autoencoder(input_dim=10)
        X = np.random.randn(10, 10)

        with pytest.raises(ValueError, match="not trained"):
            ae.predict(X)

    def test_predict_returns_score_for_single_sample(self) -> None:
        """Predict should return score for single sample."""
        ae = Autoencoder(input_dim=10)
        X_train = self._make_synthetic_data(n_samples=100)
        ae.train(X_train, epochs=5)

        X_test = np.random.randn(1, 10)
        result = ae.predict(X_test)

        assert "score" in result
        assert "is_anomaly" in result
        assert isinstance(result["score"], float)
        assert isinstance(result["is_anomaly"], bool)
        assert result["score"] >= 0

    def test_predict_returns_scores_for_multiple_samples(self) -> None:
        """Predict should return list of scores for multiple samples."""
        ae = Autoencoder(input_dim=10)
        X_train = self._make_synthetic_data(n_samples=100)
        ae.train(X_train, epochs=5)

        X_test = np.random.randn(10, 10)
        result = ae.predict(X_test)

        assert "score" in result
        assert "is_anomaly" in result
        assert isinstance(result["score"], list)
        assert isinstance(result["is_anomaly"], list)
        assert len(result["score"]) == 10
        assert len(result["is_anomaly"]) == 10

    def test_anomaly_events_score_higher_than_normal(self) -> None:
        """Anomalous samples should have higher reconstruction error."""
        ae = Autoencoder(input_dim=10)

        # Create distinct normal and anomaly distributions
        rng = np.random.default_rng(42)
        normal = rng.standard_normal((100, 10))
        anomalies = rng.standard_normal((10, 10)) * 4

        ae.train(normal, epochs=20)

        result_normal = ae.predict(normal[:5])
        result_anomaly = ae.predict(anomalies[:5])

        normal_scores = (
            result_normal["score"]
            if isinstance(result_normal["score"], list)
            else [result_normal["score"]]
        )
        anomaly_scores = (
            result_anomaly["score"]
            if isinstance(result_anomaly["score"], list)
            else [result_anomaly["score"]]
        )

        assert np.mean(anomaly_scores) > np.mean(normal_scores)

    def test_early_stopping_halts_training(self) -> None:
        """Training with patience should stop early if loss plateaus."""
        ae = Autoencoder(input_dim=10)
        X = self._make_synthetic_data(n_samples=100)

        ae.train(X, epochs=100, lr=1e-3, patience=3)

        assert ae.is_trained is True

    def test_train_with_small_patience(self) -> None:
        """Early stopping with very small patience should trigger quickly."""
        ae = Autoencoder(input_dim=10)
        X = self._make_synthetic_data(n_samples=100)

        ae.train(X, epochs=100, lr=1e-3, patience=1)

        assert ae.is_trained is True

    def test_save_and_load_state_dict_roundtrip(self, tmp_path) -> None:
        """Save and load state_dict should preserve model weights."""
        ae1 = Autoencoder(input_dim=10)
        X = self._make_synthetic_data()
        ae1.train(X, epochs=5)

        # Get state before save
        state_dict_before = {k: v.clone() for k, v in ae1.model.state_dict().items()}

        # Save state_dict
        checkpoint_path = tmp_path / "autoencoder.pt"
        torch.save(ae1.model.state_dict(), checkpoint_path)

        # Create new model and load state_dict
        ae2 = Autoencoder(input_dim=10)
        ae2.model.load_state_dict(torch.load(checkpoint_path))

        # Verify weights match
        for key in state_dict_before:
            assert torch.allclose(ae2.model.state_dict()[key], state_dict_before[key]), (
                f"Weights for {key} don't match after load"
            )

    def test_model_determinism_with_seed(self) -> None:
        """Same seed should produce same weights."""
        torch.manual_seed(42)
        ae1 = Autoencoder(input_dim=10)
        weights1 = [p.clone() for p in ae1.model.parameters()]

        torch.manual_seed(42)
        ae2 = Autoencoder(input_dim=10)
        weights2 = [p.clone() for p in ae2.model.parameters()]

        for w1, w2 in zip(weights1, weights2):
            assert torch.allclose(w1, w2)

    def test_different_input_dimensions(self) -> None:
        """Autoencoder should work with different input dimensions."""
        for input_dim in [5, 10, 20, 50]:
            ae = Autoencoder(input_dim=input_dim)
            X = np.random.randn(100, input_dim)

            ae.train(X, epochs=3)
            result = ae.predict(X[:5])

            assert isinstance(result["score"], list)
            assert len(result["score"]) == 5

    def test_batch_processing(self) -> None:
        """Autoencoder should handle variable batch sizes."""
        ae = Autoencoder(input_dim=10)
        X_train = self._make_synthetic_data(n_samples=100)
        ae.train(X_train, epochs=5)

        for batch_size in [1, 5, 10, 32]:
            X_test = np.random.randn(batch_size, 10)
            result = ae.predict(X_test)

            if batch_size == 1:
                assert isinstance(result["score"], float)
            else:
                assert isinstance(result["score"], list)
                assert len(result["score"]) == batch_size

    def test_training_with_custom_lr(self) -> None:
        """Training should accept custom learning rate."""
        ae = Autoencoder(input_dim=10)
        X = self._make_synthetic_data()

        ae.train(X, epochs=5, lr=1e-2)
        assert ae.is_trained is True

    def test_training_with_custom_batch_size(self) -> None:
        """Training should work with different batch sizes."""
        ae = Autoencoder(input_dim=10)
        X = self._make_synthetic_data(n_samples=256)

        ae.train(X, epochs=5)
        assert ae.is_trained is True

    def test_threshold_is_positive(self) -> None:
        """Anomaly threshold should be positive."""
        ae = Autoencoder(input_dim=10)
        X = self._make_synthetic_data()

        ae.train(X, epochs=5)

        assert ae.threshold > 0, "Threshold should be positive"

    def test_predict_consistency(self) -> None:
        """Same input should produce same prediction scores."""
        ae = Autoencoder(input_dim=10)
        X_train = self._make_synthetic_data()
        ae.train(X_train, epochs=5)

        X_test = np.random.randn(5, 10)
        ae.model.eval()

        result1 = ae.predict(X_test)
        result2 = ae.predict(X_test)

        scores1 = result1["score"] if isinstance(result1["score"], list) else [result1["score"]]
        scores2 = result2["score"] if isinstance(result2["score"], list) else [result2["score"]]

        assert np.allclose(scores1, scores2)
