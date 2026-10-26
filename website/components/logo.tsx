export function LogoMark({ className = "size-7" }: { className?: string }) {
  return (
    <svg viewBox="0 0 256 256" className={className} aria-hidden>
      <defs>
        <linearGradient id="mmq-moon" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0" stopColor="#8B8FF7" />
          <stop offset="1" stopColor="#3B3DA8" />
        </linearGradient>
        <mask id="mmq-crescent">
          <circle cx="122" cy="128" r="100" fill="#fff" />
          <circle cx="166" cy="104" r="84" fill="#000" />
        </mask>
        <clipPath id="mmq-lanes">
          {[28, 64, 100, 136, 172, 208].map((y) => (
            <rect key={y} x="0" y={y} width="256" height="30" rx="4" />
          ))}
        </clipPath>
      </defs>
      <g clipPath="url(#mmq-lanes)">
        <rect width="256" height="256" fill="url(#mmq-moon)" mask="url(#mmq-crescent)" />
      </g>
      <circle cx="200" cy="84" r="7" fill="#FBE0A6" />
      <circle cx="176" cy="112" r="9" fill="#F8CB6C" />
      <circle cx="148" cy="142" r="12" fill="#F5B841" />
    </svg>
  );
}

export function Logo() {
  return (
    <span className="flex items-center gap-2.5">
      <LogoMark />
      <span className="text-lg font-semibold tracking-tight">
        Moon<span className="text-brand-light">MQ</span>
      </span>
    </span>
  );
}
