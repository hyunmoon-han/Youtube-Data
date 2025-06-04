# import torch
# from transformers import AutoTokenizer, AutoModelForSequenceClassification

# model_name = "snunlp/KR-FinBert-SC"
# tokenizer = AutoTokenizer.from_pretrained(model_name)
# model = AutoModelForSequenceClassification.from_pretrained(model_name)

# texts = [
#     "정말 좋은제품이네요 추천합니다!!",
#     "완전 별로에요 다시는 안 살 거예요.",
#     "그럭저럭 무난해요.",
# ]

# for text in texts:
#     inputs = tokenizer(text, return_tensors="pt")
#     with torch.no_grad():
#         outputs = model(**inputs)
#     logits = outputs.logits
#     probs = torch.softmax(logits, dim=1)
#     predicted_class_idx = torch.argmax(probs).item()
#     label = model.config.id2label[predicted_class_idx]
#     print(f"문장: {text}")
#     print(f"로짓: {logits}")
#     print(f"확률: {probs}")
#     print(f"예측 라벨: {label}\n")
from transformers import pipeline

model_name = "cardiffnlp/twitter-roberta-base-sentiment"
classifier = pipeline("sentiment-analysis", model=model_name)

# 모델 config에서 라벨 이름 가져오기
id2label = classifier.model.config.id2label
# 예: {0: 'negative', 1: 'neutral', 2: 'positive'}

def pretty_result(text):
    results = classifier(text)
    for res in results:
        label_id = None
        # pipeline 결과 label은 보통 'LABEL_0' 이런 식이라서 숫자만 추출
        if res['label'].startswith("LABEL_"):
            label_id = int(res['label'].split('_')[1])
            label = id2label.get(label_id, res['label'])
        else:
            label = res['label']  # 이미 라벨명인 경우

        score = res['score']

        print(f"입력 문장: {text}")
        print(f"감성 라벨: {label}")
        print(f"신뢰도 점수: {score:.4f}")
        print("-" * 30)

# 테스트
pretty_result("이 제품 정말 좋아요 추천합니다!")
pretty_result("최악이에요 다시는 안 살거예요")
pretty_result("그냥 그래요 뭐 특별하지는 않네요")
