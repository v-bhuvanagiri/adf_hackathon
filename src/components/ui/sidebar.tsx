'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'

const navItems = [
  { name: 'Dashboard', path: '/' },
  { name: 'Stores', path: '/stores' },
  { name: 'Inventory', path: '/inventory' },
  { name: 'Forecasts', path: '/forecasts' },
]

export default function Sidebar() {
  const pathname = usePathname()

  return (
    <nav className="w-[20%] h-screen sticky top-0 bg-gray-800">
      <div className="p-5">
        <h2 className="text-white text-2xl font-semibold mb-5">Walmart Sparkathon</h2>
        <ul>
          {navItems.map((item) => (
            <li key={item.path} className="mb-2">
              <Link 
                href={item.path}
                className={`block p-2 rounded ${
                  pathname === item.path 
                    ? 'bg-blue-500 text-white' 
                    : 'text-gray-300 hover:bg-gray-700'
                }`}
              >
                {item.name}
              </Link>
            </li>
          ))}
        </ul>
      </div>
    </nav>
  )
}