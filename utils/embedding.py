from __future__ import annotations
from pathlib import Path
from typing import Optional, Sequence, Union
import os
import numpy as np
from sentence_transformers import SentenceTransformer


ModelRef = Union[str, Path]


def _download_embedding_model_to_dir(
    repo_id: str,
    local_dir: Union[str, Path],
    hf_endpoint: Optional[str] = None,
) -> None:
    """从 HuggingFace 下载模型到指定目录。若 hf_endpoint 非空则用作镜像（显式传给 HfApi），失败则抛错。"""
    local_dir = Path(local_dir).expanduser().resolve()
    local_dir.mkdir(parents=True, exist_ok=True)
    endpoint = (str(hf_endpoint).strip().rstrip("/")) if hf_endpoint and str(hf_endpoint).strip() else None
    from huggingface_hub import HfApi
    api = HfApi(endpoint=endpoint)
    api.snapshot_download(
        repo_id=repo_id,
        local_dir=str(local_dir),
    )
    # 必须确认目录内已有模型文件，否则说明下载未真正落到此目录
    if not any(local_dir.iterdir()):
        raise RuntimeError(
            f"下载后目录仍为空: {local_dir}，请检查网络或 hf_endpoint 配置（如 https://hf-mirror.com）"
        )


class EmbeddingTool:
    def __init__(
        self,
        model_name: str = "BAAI/bge-small-zh-v1.5",
        *,
        model_path: Optional[ModelRef] = None,
        hf_endpoint: Optional[str] = None,
        normalize_embeddings: bool = True,
        batch_size: int = 32,
        device: Optional[str] = None,
        cache_dir: Optional[ModelRef] = None,
        local_files_only: bool = False,
        max_length: int = 512,
        trust_remote_code: bool = False,
    ):
        """
        支持加载 HuggingFace 模型名或本地模型目录。
        若 model_path 指向的目录不存在，会从 HuggingFace（或 hf_endpoint 配置的镜像）下载到该目录。

        Args:
            model_name: HF 模型 id（例如 "BAAI/bge-small-zh-v1.5"）
            model_path: 本地模型目录路径；不存在时会自动下载到此目录
            hf_endpoint: HuggingFace 镜像或 API 地址（如 https://hf-mirror.com），空则用默认
            normalize_embeddings: 是否归一化 embedding（推荐检索场景开启）
            batch_size: 批量向量化 batch size
            device: "cpu"/"cuda" 等，None 则由 sentence-transformers 自动选择
            cache_dir: 可选 HF cache 目录
            local_files_only: True 时禁止下载，只使用本地文件
            max_length: transformers fallback 编码的最大长度（token）
            trust_remote_code: transformers fallback 是否允许远程代码
        """
        self.model_name = model_name
        self.model_path = model_path
        self.normalize_embeddings = normalize_embeddings
        self.batch_size = batch_size
        self.device = device
        self.cache_dir = cache_dir
        self.local_files_only = local_files_only
        self.max_length = max_length
        self.trust_remote_code = trust_remote_code

        # Keep startup logs clean: transformers may emit INFO-level "LOAD REPORT" for
        # harmless unexpected keys (e.g. embeddings.position_ids). Default to WARNING.
        # Override with env: ASKDB_HF_VERBOSITY=INFO|WARNING|ERROR
        _configure_hf_verbosity()

        model_ref: str
        if model_path is not None:
            p = Path(str(model_path)).expanduser().resolve()
            if not p.exists():
                if local_files_only:
                    raise RuntimeError(
                        f"本地模型目录不存在且禁止下载: {p}，请先下载模型到该目录或移除 local_files_only"
                    )
                _download_embedding_model_to_dir(
                    repo_id=model_name,
                    local_dir=p,
                    hf_endpoint=str(hf_endpoint).strip() if hf_endpoint else None,
                )
            if not p.exists() or not any(p.iterdir()):
                raise RuntimeError(
                    f"模型目录无效或为空: {p}，无法加载 embedding 模型，也不会生成 pkl"
                )
            model_ref = str(p)
        else:
            model_ref = model_name

        # If it looks like an existing local path, use it directly.
        try:
            pp = Path(model_ref).expanduser()
            if pp.exists():
                model_ref = str(pp)
        except Exception:
            pass

        st_kwargs = {
            "cache_folder": str(cache_dir) if cache_dir is not None else None,
            "local_files_only": local_files_only,
        }
        st_kwargs = {k: v for k, v in st_kwargs.items() if v is not None}

        self.backend: str
        self.model: object
        self._tokenizer = None

        try:
            if device:
                self.model = SentenceTransformer(model_ref, device=device, **st_kwargs)
            else:
                self.model = SentenceTransformer(model_ref, **st_kwargs)
            self.backend = "sentence-transformers"
        except Exception as st_exc:
            # Fallback: use transformers + mean pooling
            try:
                import torch
                from transformers import AutoModel, AutoTokenizer
                import torch.nn.functional as F
            except Exception as imp_exc:  # pragma: no cover
                raise RuntimeError(
                    "SentenceTransformer load failed and transformers fallback is unavailable. "
                    "Please install `transformers` and `torch`, or provide a valid sentence-transformers model directory."
                ) from imp_exc

            # When model_ref is a local path that doesn't exist, HF expects a repo id (e.g. "BAAI/bge-large-zh-v1.5").
            # Use model_name so the model can be downloaded on first use.
            hf_ref = model_ref
            try:
                if not Path(model_ref).exists():
                    hf_ref = model_name
            except Exception:
                hf_ref = model_name

            cache_dir_str = str(cache_dir) if cache_dir is not None else None
            tok_kwargs = {
                "cache_dir": cache_dir_str,
                "local_files_only": local_files_only,
                "trust_remote_code": trust_remote_code,
            }
            tok_kwargs = {k: v for k, v in tok_kwargs.items() if v is not None}
            self._tokenizer = AutoTokenizer.from_pretrained(hf_ref, **tok_kwargs)

            model_kwargs = {
                "cache_dir": cache_dir_str,
                "local_files_only": local_files_only,
                "trust_remote_code": trust_remote_code,
            }
            model_kwargs = {k: v for k, v in model_kwargs.items() if v is not None}
            hf_model = AutoModel.from_pretrained(hf_ref, **model_kwargs)

            # device selection
            if device:
                dev = torch.device(device)
            else:
                dev = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            hf_model.to(dev)
            hf_model.eval()

            self.model = hf_model
            self._torch = torch
            self._F = F
            self._device = dev
            self.backend = "transformers-mean-pooling"
            # keep original exception for debugging if needed
            self._st_load_error = st_exc

    def _encode_with_transformers(self, texts: Sequence[str], batch_size: int) -> np.ndarray:
        torch = self._torch
        F = self._F
        tokenizer = self._tokenizer
        model = self.model

        if tokenizer is None:  # pragma: no cover
            raise RuntimeError("transformers tokenizer is not initialized")

        all_vecs: list[np.ndarray] = []
        for i in range(0, len(texts), batch_size):
            batch = list(texts[i : i + batch_size])
            inputs = tokenizer(
                batch,
                padding=True,
                truncation=True,
                max_length=self.max_length,
                return_tensors="pt",
            )
            inputs = {k: v.to(self._device) for k, v in inputs.items()}
            with torch.no_grad():
                out = model(**inputs)
                token_emb = out.last_hidden_state  # (b, s, h)
                mask = inputs.get("attention_mask")
                if mask is None:
                    # assume all tokens valid
                    pooled = token_emb.mean(dim=1)
                else:
                    mask_f = mask.unsqueeze(-1).type_as(token_emb)
                    summed = (token_emb * mask_f).sum(dim=1)
                    denom = mask_f.sum(dim=1).clamp(min=1e-6)
                    pooled = summed / denom

                if self.normalize_embeddings:
                    pooled = F.normalize(pooled, p=2, dim=1)
            all_vecs.append(pooled.detach().cpu().numpy().astype(np.float32))

        if not all_vecs:
            # dim unknown without a forward pass; return empty
            return np.zeros((0, 0), dtype=np.float32)
        return np.concatenate(all_vecs, axis=0)

    def embed(self, text: str) -> np.ndarray:
        """Embed a single text into a 1-D vector (float32)."""
        if text is None:
            raise ValueError("text must not be None")
        arr2 = self.embed_batch([text])
        return arr2[0] if len(arr2) else np.zeros((0,), dtype=np.float32)

    def embed_batch(self, texts: Sequence[str]) -> np.ndarray:
        """Embed a batch of texts into a 2-D array (n, dim) float32."""
        if texts is None:
            raise ValueError("texts must not be None")
        if len(texts) == 0:
            return np.zeros((0, 0), dtype=np.float32)

        if self.backend == "sentence-transformers":
            vecs = self.model.encode(  # type: ignore[attr-defined]
                list(texts),
                normalize_embeddings=self.normalize_embeddings,
                batch_size=self.batch_size,
                show_progress_bar=False,
            )
            return np.asarray(vecs, dtype=np.float32)

        return self._encode_with_transformers(texts, batch_size=self.batch_size)

    def get_similarity(self, text: str, embedding: object) -> float:
        """
        计算文本向量与“列向量”的相似度（cosine / inner-product）。

        - `embedding` 支持：
          - `np.ndarray` (dim,)
          - `dict` payload（例如 pickle 中包含 {"embedding": np.ndarray, ...}）
          - `list[float]`
        """
        if text is None:
            raise ValueError("text must not be None")

        emb_obj = embedding
        if isinstance(emb_obj, dict) and "embedding" in emb_obj:
            emb_obj = emb_obj.get("embedding")

        emb_vec = np.asarray(emb_obj, dtype=np.float32)
        if emb_vec.ndim > 1:
            emb_vec = emb_vec.reshape(-1)

        text_vec = self.embed(text)
        if text_vec.ndim > 1:
            text_vec = text_vec.reshape(-1)

        # If both sides are normalized, cosine == dot
        if self.normalize_embeddings:
            return float(np.dot(text_vec, emb_vec))

        denom = (np.linalg.norm(text_vec) * np.linalg.norm(emb_vec)) + 1e-12
        return float(np.dot(text_vec, emb_vec) / denom)


def _configure_hf_verbosity() -> None:
    level = str(os.environ.get("ASKDB_HF_VERBOSITY") or "").strip().upper()
    if not level:
        level = "WARNING"
    try:
        from transformers.utils import logging as tf_logging

        if level == "INFO":
            tf_logging.set_verbosity_info()
        elif level == "ERROR":
            tf_logging.set_verbosity_error()
        else:
            tf_logging.set_verbosity_warning()
    except Exception:
        pass
    try:
        from huggingface_hub.utils import logging as hub_logging

        if level == "INFO":
            hub_logging.set_verbosity_info()
        elif level == "ERROR":
            hub_logging.set_verbosity_error()
        else:
            hub_logging.set_verbosity_warning()
    except Exception:
        pass

__all__ = ["EmbeddingTool"]
