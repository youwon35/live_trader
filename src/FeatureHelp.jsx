import { useId, useRef } from "react";
import { CircleHelp } from "lucide-react";
import "./FeatureHelp.css";

export function FeatureHelp({ title, description, details = [], children }) {
  const titleId = useId();
  const dialog = useRef(null);

  return (
    <>
      <button
        type="button"
        className="secondary-button feature-help-trigger"
        aria-haspopup="dialog"
        onClick={() => dialog.current?.showModal()}
      >
        <CircleHelp size={15} aria-hidden="true" />
        기능 설명
      </button>
      <dialog ref={dialog} className="feature-help-dialog" aria-labelledby={titleId}>
        <h2 id={titleId}>{title} 기능 설명</h2>
        <p>{description}</p>
        {details.map((detail) => <p key={detail}>{detail}</p>)}
        {children}
        <form method="dialog">
          <button type="submit" className="secondary-button">닫기</button>
        </form>
      </dialog>
    </>
  );
}
