from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


def load_llm(
    base_model_id: str,
    adapter_path: str | None,
    max_new_tokens: int = 1024,
) -> tuple[Any, Any]:
    """
    Load tokenizer and model, optionally apply a LoRA adapter, and return
    (tokenizer, HuggingFacePipeline) for use in the pipeline engine.

    Parameters
    ----------
    base_model_id:
        HuggingFace hub ID or local path (e.g. /model for bind-mount).
    adapter_path:
        Local path to a PEFT adapter directory. Pass None to skip (base model only).
    max_new_tokens:
        Hard cap on tokens generated per call.
    """
    import torch
    from langchain_community.llms import HuggingFacePipeline
    from peft import PeftModel
    from transformers import (
        AutoModelForCausalLM,
        AutoTokenizer,
        BitsAndBytesConfig,
        pipeline,
    )

    log.info("Loading tokenizer: %s", base_model_id)
    tokenizer = AutoTokenizer.from_pretrained(base_model_id, use_fast=True)
    tokenizer.pad_token = tokenizer.eos_token

    cuda_available = torch.cuda.is_available()

    if cuda_available:
        log.info("Loading base model: %s (4-bit NF4, CUDA)", base_model_id)
        bnb_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.float16,
            bnb_4bit_use_double_quant=True,
        )
        base_model = AutoModelForCausalLM.from_pretrained(
            base_model_id,
            quantization_config=bnb_config,
            device_map="cuda:0",
            dtype=torch.float16,
            trust_remote_code=False,
        )
    else:
        log.warning(
            "CUDA not available — loading model in float32 on CPU. "
            "Inference will be slow; enable GPU for production use."
        )
        base_model = AutoModelForCausalLM.from_pretrained(
            base_model_id,
            device_map="cpu",
            torch_dtype=torch.float32,
            trust_remote_code=False,
        )

    if adapter_path:
        log.info("Applying LoRA adapter: %s", adapter_path)
        model = PeftModel.from_pretrained(base_model, adapter_path)
    else:
        log.warning("No ADAPTER_PATH set — running base model without fine-tuning.")
        model = base_model

    hf_pipe = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        max_new_tokens=max_new_tokens,
        do_sample=False,
        temperature=None,
        top_p=None,
        return_full_text=False,
        pad_token_id=tokenizer.eos_token_id,
    )

    llm = HuggingFacePipeline(pipeline=hf_pipe)
    log.info(
        "Model ready — base=%s  adapter=%s  max_new_tokens=%d",
        base_model_id,
        adapter_path or "none",
        max_new_tokens,
    )
    return tokenizer, llm
